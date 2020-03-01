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
    return await this.knex.schema.hasTable(tableName);
  }

  async getSchema(tableName) {
    return await this.knex(tableName).columnInfo();
  }

  // Snippet taken from: https://github.com/knex/knex/issues/360#issuecomment-406483016
  async listTables() {
    let query;
    let bindings;

    switch (this.knex.client.constructor.name) {
      case "Client_MSSQL":
        query =
          "SELECT table_name FROM information_schema.tables WHERE table_schema = schema_name() AND table_type = 'BASE TABLE' AND table_catalog = ?";
        bindings = [this.knex.client.database()];
        break;
      case "Client_MySQL":
      case "Client_MySQL2":
        query =
          "SELECT table_name AS table_name FROM information_schema.tables WHERE table_schema = ?";
        bindings = [this.knex.client.database()];
        break;
      case "Client_Oracle":
      case "Client_Oracledb":
        query = "SELECT table_name FROM user_tables";
        break;
      case "Client_PG":
        query =
          "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_catalog = ?";
        bindings = [this.knex.client.database()];
        break;
      case "Client_SQLite3":
        query =
          "SELECT name AS table_name FROM sqlite_master WHERE type='table'";
        break;
    }

    let rows = await this.knex.raw(query, bindings);

    // SQLite and MSSQL return the resulting rows directly
    if (
      this.knex.client.constructor.name !== "Client_SQLite3" &&
      this.knex.client.constructor.name !== "Client_MSSQL"
    ) {
      rows = rows[0];
    }

    return rows.map(r => r.table_name);
  }

  async getTableInfo(tableName) {
    const [result] = await this.knex("information_schema.tables")
      .where({
        table_schema: this.knex.client.database(),
        table_name: tableName
      })
      .select({
        bytes: this.knex.raw("data_length + index_length"),
        rows: "table_rows"
      });

    return { bytes: result.bytes, rows: result.rows };
  }
}

module.exports = Lib;
