const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class SqliteSchemaBuilder extends BaseSchemaBuilder {
  async getPrimaryKey(table) {
    return this.knex(this.knex.raw(`pragma_table_info('${table}')`))
      .where("pk", "!=", 0)
      .orderBy("pk")
      .pluck("name");
  }

  async _rawListTables() {
    // Using dbstat virtual table. Requires sqlite3 to be compiled
    // with SQLITE_ENABLE_DBSTAT_VTAB=1, which is already available
    // in the precompiled binaries since v4.3 of sqlite3.
    // See: https://github.com/mapbox/node-sqlite3/issues/1279
    return this.knex("dbstat as s")
      .join("sqlite_master as t", "s.name", "=", "t.name")
      .whereRaw("s.name not like 'sqlite_%' and t.type = 'table'")
      .orderBy("s.name")
      .select({
        table: "s.name",
        rows: this.knex.raw("SUM(s.ncell)"),
        bytes: this.knex.raw("SUM(s.pgsize)"),
      })
      .groupBy("s.name");
  }

  async _rawListIndexes(table) {
    // Avoid rows without sql, to ignore any autoindexes created
    // by sqlite on the table.
    return this.knex("sqlite_master")
      .whereNotNull("sql")
      .andWhere({ type: "index", tbl_name: table })
      .select({
        name: "name",
        sql: "sql",
        unique: this.knex.raw(`instr(sql, "CREATE UNIQUE")`),
      });
  }

  async _rawListForeignKeys(table) {
    return this.knex(this.knex.raw(`pragma_foreign_key_list('${table}')`));
  }

  async _rawListDatabases() {
    return this.knex("pragma_database_list").pluck("name");
  }
}

module.exports = SqliteSchemaBuilder;
