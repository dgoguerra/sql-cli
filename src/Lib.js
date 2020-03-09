const _ = require("lodash");
const Knex = require("knex");

class Lib {
  constructor({ knex }) {
    if (knex.name === "knex") {
      this.knexIsInternal = false;
      this.knex = knex;
    } else {
      this.knexIsInternal = true;
      this.knex = Knex(knex);
    }
  }

  async checkConnection() {
    await this.knex.raw("select 1+1 as result");
  }

  async destroy() {
    if (this.knexIsInternal) {
      await this.knex.destroy();
    }
  }

  async tableExists(tableName) {
    if (this.knex.client.constructor.name === "Client_BigQuery") {
      return true;
    }
    return await this.knex.schema.hasTable(tableName);
  }

  async getSchema(tableName) {
    const client = this.knex.client.constructor.name;
    const database = this.knex.client.database();

    if (client === "Client_BigQuery") {
      const results = await this.knex("INFORMATION_SCHEMA.COLUMNS")
        .where({ table_schema: database, table_name: tableName })
        .select({
          column: "column_name",
          nullable: "is_nullable",
          type: "data_type"
        });

      return _.transform(
        results,
        (acc, row) => {
          acc[row.column] = {
            nullable: row.nullable === "YES",
            type: row.type
          };
        },
        {}
      );
    }

    return await this.knex(tableName).columnInfo();
  }

  // Snippet taken from: https://github.com/knex/knex/issues/360#issuecomment-406483016
  async listTables() {
    const client = this.knex.client.constructor.name;
    const database = this.knex.client.database();

    if (client === "Client_MySQL" || client === "Client_MySQL2") {
      return await this.knex("information_schema.tables")
        .where({ table_schema: database })
        .select({
          table: "table_name",
          bytes: this.knex.raw("data_length + index_length"),
          rows: "table_rows"
        });
    }

    if (client === "Client_PG") {
      return await this.knex("information_schema.tables")
        .where({
          table_schema: this.knex.raw("current_schema()"),
          table_catalog: database
        })
        .select({ table: "table_name" });
    }

    if (client === "Client_SQLite3") {
      return await this.knex("sqlite_master")
        .where({ type: "table" })
        .select({ table: "name" });
    }

    if (client === "Client_MSSQL") {
      // TODO no funciona
      /*
      const rows = await this.knex.raw(`SELECT t.NAME AS table,
        s.Name AS schema,
        p.rows,
        SUM(a.used_pages) * 8 AS usedSpaceKB
      FROM sys.tables t
        INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE
        t.NAME NOT LIKE 'dt%' AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255
      GROUP BY t.Name, s.Name, p.Rows ORDER BY t.Name;
      `);

      return rows.map(row => ({
        ...row,
        rows: Number(row.rows),
        bytes: Number(row.usedSpaceKB / 1000)
      }));
      */

      return await this.knex("information_schema.tables")
        .where({
          table_schema: this.knex.raw("schema_name()"),
          table_type: "BASE TABLE",
          table_catalog: database
        })
        .select({ table: "table_name" });
    }

    if (client === "Client_Oracle" || client === "Client_Oracledb") {
      return await this.knex("user_tables").select({ table: "table_name" });
    }

    if (client === "Client_BigQuery") {
      return await this.knex("__TABLES__")
        .where({ dataset_id: database, type: 1 })
        .select({ table: "table_id", bytes: "size_bytes", rows: "row_count" });
    }

    throw new Error(`Unexpected client '${client}', not implemented`);
  }
}

module.exports = Lib;
