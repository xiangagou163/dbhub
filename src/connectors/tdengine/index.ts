import * as http from "http";
import * as https from "https";
import {
  Connector,
  ConnectorType,
  ConnectorRegistry,
  DSNParser,
  SQLResult,
  TableColumn,
  TableIndex,
  StoredProcedure,
  ExecuteOptions,
  ConnectorConfig,
} from "../interface.js";
import { SafeURL } from "../../utils/safe-url.js";
import { obfuscateDSNPassword } from "../../utils/dsn-obfuscate.js";
import { SQLRowLimiter } from "../../utils/sql-row-limiter.js";
import { isReadOnlySQL } from "../../utils/allowed-keywords.js";

interface TDengineConnectionInfo {
  host: string;
  port: number;
  database?: string;
  user: string;
  password: string;
  useTLS: boolean;
}

interface TDengineRestResponse {
  code: number;
  desc?: string;
  column_meta?: Array<[string, string, number]>;
  data?: any[];
  rows?: number;
}

/**
 * TDengine DSN Parser
 * Handles DSN strings like: tdengine://user:password@localhost:6041/dbname?sslmode=require
 *
 * Uses taosAdapter REST API as the transport layer.
 */
class TDengineDSNParser implements DSNParser {
  async parse(dsn: string, config?: ConnectorConfig): Promise<TDengineConnectionInfo> {
    // Basic validation
    if (!this.isValidDSN(dsn)) {
      const obfuscatedDSN = obfuscateDSNPassword(dsn);
      const expectedFormat = this.getSampleDSN();
      throw new Error(
        `Invalid TDengine DSN format.\nProvided: ${obfuscatedDSN}\nExpected: ${expectedFormat}`
      );
    }

    try {
      const url = new SafeURL(dsn);
      const useTLS = url.getSearchParam("sslmode") === "require";
      const port = url.port ? parseInt(url.port, 10) : 6041;
      const database = url.pathname ? url.pathname.substring(1) : "";

      if (!url.hostname) {
        throw new Error("TDengine DSN requires a hostname.");
      }
      if (!url.username || !url.password) {
        throw new Error("TDengine DSN requires username and password.");
      }

      return {
        host: url.hostname,
        port,
        database: database || undefined,
        user: url.username,
        password: url.password,
        useTLS,
      };
    } catch (error) {
      throw new Error(
        `Failed to parse TDengine DSN: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }

  getSampleDSN(): string {
    return "tdengine://root:taosdata@localhost:6041/metrics";
  }

  isValidDSN(dsn: string): boolean {
    try {
      return dsn.startsWith("tdengine://");
    } catch (error) {
      return false;
    }
  }
}

function escapeLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

/**
 * TDengine Connector Implementation (REST API via taosAdapter)
 */
export class TDengineConnector implements Connector {
  id: ConnectorType = "tdengine";
  name = "TDengine";
  dsnParser = new TDengineDSNParser();

  private connectionInfo: TDengineConnectionInfo | null = null;
  private authHeader: string | null = null;
  private queryTimeoutMs?: number;
  private connectionTimeoutMs?: number;

  // Source ID is set by ConnectorManager after cloning
  private sourceId: string = "default";

  getId(): string {
    return this.sourceId;
  }

  clone(): Connector {
    return new TDengineConnector();
  }

  async connect(dsn: string, initScript?: string, config?: ConnectorConfig): Promise<void> {
    try {
      const connectionInfo = await this.dsnParser.parse(dsn, config);
      this.connectionInfo = connectionInfo;
      this.authHeader = this.buildAuthHeader(connectionInfo.user, connectionInfo.password);

      if (config?.queryTimeoutSeconds !== undefined) {
        this.queryTimeoutMs = config.queryTimeoutSeconds * 1000;
      }
      if (config?.connectionTimeoutSeconds !== undefined) {
        this.connectionTimeoutMs = config.connectionTimeoutSeconds * 1000;
      }

      // Test the connection
      await this.executeRaw("SELECT 1", this.connectionTimeoutMs ?? this.queryTimeoutMs);
    } catch (err) {
      console.error("Failed to connect to TDengine database:", err);
      throw err;
    }
  }

  async disconnect(): Promise<void> {
    this.connectionInfo = null;
    this.authHeader = null;
  }

  async getSchemas(): Promise<string[]> {
    const rows = await this.queryRows(
      "SELECT name FROM information_schema.ins_databases ORDER BY name"
    );
    return rows.map((row) => row.name);
  }

  async getTables(schema?: string): Promise<string[]> {
    const database = schema || this.connectionInfo?.database;
    if (!database) {
      throw new Error("TDengine requires a database name to list tables.");
    }

    const rows = await this.queryRows(
      `SELECT table_name FROM information_schema.ins_tables WHERE db_name = '${escapeLiteral(
        database
      )}' ORDER BY table_name`
    );
    return rows.map((row) => row.table_name);
  }

  async tableExists(tableName: string, schema?: string): Promise<boolean> {
    const database = schema || this.connectionInfo?.database;
    if (!database) {
      throw new Error("TDengine requires a database name to check table existence.");
    }

    const rows = await this.queryRows(
      `SELECT table_name FROM information_schema.ins_tables WHERE db_name = '${escapeLiteral(
        database
      )}' AND table_name = '${escapeLiteral(tableName)}' LIMIT 1`
    );
    return rows.length > 0;
  }

  async getTableIndexes(tableName: string, schema?: string): Promise<TableIndex[]> {
    return [];
  }

  async getTableSchema(tableName: string, schema?: string): Promise<TableColumn[]> {
    const database = schema || this.connectionInfo?.database;
    if (!database) {
      throw new Error("TDengine requires a database name to describe tables.");
    }

    const rows = await this.queryRows(
      `SELECT col_name, col_type, col_nullable FROM information_schema.ins_columns WHERE db_name = '${escapeLiteral(
        database
      )}' AND table_name = '${escapeLiteral(tableName)}' ORDER BY col_name`
    );

    return rows.map((row) => ({
      column_name: row.col_name,
      data_type: row.col_type,
      is_nullable: row.col_nullable === 1 ? "YES" : "NO",
      column_default: null,
    }));
  }

  async getStoredProcedures(schema?: string): Promise<string[]> {
    return [];
  }

  async getStoredProcedureDetail(procedureName: string, schema?: string): Promise<StoredProcedure> {
    throw new Error("TDengine does not support stored procedures.");
  }

  async executeSQL(sql: string, options: ExecuteOptions, parameters?: any[]): Promise<SQLResult> {
    if (!this.connectionInfo) {
      throw new Error("Not connected to database");
    }
    if (parameters && parameters.length > 0) {
      throw new Error("TDengine REST connector does not support parameterized queries.");
    }

    const statements = sql
      .split(";")
      .map((statement) => statement.trim())
      .filter((statement) => statement.length > 0);

    let allRows: any[] = [];
    let totalRowCount = 0;

    for (const statement of statements) {
      const isQuery = isReadOnlySQL(statement, this.id);
      const processedStatement = isQuery
        ? SQLRowLimiter.applyMaxRows(statement, options.maxRows)
        : statement;
      const response = await this.executeRaw(processedStatement, this.queryTimeoutMs);

      if (isQuery) {
        const rows = this.normalizeRows(response);
        const rowCount = typeof response.rows === "number" ? response.rows : rows.length;
        allRows.push(...rows);
        totalRowCount += rowCount;
      } else {
        totalRowCount += this.extractAffectedRows(response);
      }
    }

    return { rows: allRows, rowCount: totalRowCount };
  }

  private buildAuthHeader(user: string, password: string): string {
    const encoded = Buffer.from(`${user}:${password}`).toString("base64");
    return `Basic ${encoded}`;
  }

  private async queryRows(sql: string): Promise<any[]> {
    const response = await this.executeRaw(sql, this.queryTimeoutMs);
    return this.normalizeRows(response);
  }

  private normalizeRows(response: TDengineRestResponse): any[] {
    const data = response.data ?? [];
    if (data.length === 0) {
      return [];
    }

    if (!Array.isArray(data[0])) {
      return data;
    }

    if (!response.column_meta) {
      return data;
    }

    const columns = response.column_meta.map((meta) => meta[0]);
    return data.map((row) => {
      const mapped: Record<string, any> = {};
      columns.forEach((name, index) => {
        mapped[name] = row[index];
      });
      return mapped;
    });
  }

  private extractAffectedRows(response: TDengineRestResponse): number {
    const rows = this.normalizeRows(response);
    if (rows.length === 0) {
      return 0;
    }

    const first = rows[0] as Record<string, any>;
    const affected = first.affected_rows;
    if (typeof affected === "number") {
      return affected;
    }
    if (typeof affected === "string" && affected !== "") {
      return Number(affected);
    }
    return 0;
  }

  private async executeRaw(sql: string, timeoutMs?: number): Promise<TDengineRestResponse> {
    if (!this.connectionInfo || !this.authHeader) {
      throw new Error("Not connected to database");
    }

    const { host, port, useTLS } = this.connectionInfo;
    const requestPath = "/rest/sql";
    const body = sql;
    const headers = {
      "Content-Type": "text/plain; charset=utf-8",
      "Accept": "application/json",
      "Authorization": this.authHeader,
      "Content-Length": Buffer.byteLength(body).toString(),
    };

    const client = useTLS ? https : http;

    return new Promise((resolve, reject) => {
      const request = client.request(
        {
          hostname: host,
          port,
          path: requestPath,
          method: "POST",
          headers,
        },
        (response) => {
          let payload = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => {
            payload += chunk;
          });
          response.on("end", () => {
            try {
              const parsed = JSON.parse(payload) as TDengineRestResponse;
              if (parsed.code !== 0) {
                return reject(
                  new Error(`TDengine error ${parsed.code}: ${parsed.desc || "unknown error"}`)
                );
              }
              if (response.statusCode && response.statusCode >= 400) {
                return reject(
                  new Error(`TDengine HTTP error ${response.statusCode}: ${response.statusMessage || "unknown"}`)
                );
              }
              resolve(parsed);
            } catch (error) {
              reject(
                new Error(
                  `Failed to parse TDengine response: ${error instanceof Error ? error.message : String(error)}`
                )
              );
            }
          });
        }
      );

      request.on("error", (error) => reject(error));

      if (timeoutMs) {
        request.setTimeout(timeoutMs, () => {
          request.destroy(new Error(`TDengine request timeout after ${timeoutMs}ms`));
        });
      }

      request.write(body);
      request.end();
    });
  }
}

// Create and register the connector
const tdengineConnector = new TDengineConnector();
ConnectorRegistry.register(tdengineConnector);
