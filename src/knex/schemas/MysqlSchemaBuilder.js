const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class MysqlSchemaBuilder extends BaseSchemaBuilder {
  async getPrimaryKey(table) {
    const database = this.knex.client.database();

    return this.knex("information_schema.statistics")
      .where({
        table_schema: database,
        table_name: table,
        index_name: "PRIMARY",
      })
      .pluck("COLUMN_NAME");
  }

  async _rawListColumns(table) {
    const database = this.knex.client.database();

    return this.knex("information_schema.columns")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        name: "column_name",
        nullable: "is_nullable",
        type: "data_type",
        default: "column_default",
        maxLength: "character_maximum_length",
        unsigned: this.knex.raw(`instr(column_type, "unsigned")`),
        precision: "numeric_precision",
        scale: "numeric_scale",
      });
  }

  async _rawListTables() {
    const database = this.knex.client.database();

    return this.knex("information_schema.tables")
      .where({ table_schema: database })
      .orderBy("table_name")
      .select({
        table: "table_name",
        bytes: this.knex.raw("data_length + index_length"),
        rows: "table_rows",
      });
  }

  async _rawListIndexes(table) {
    const database = this.knex.client.database();

    const rows = await this.knex("information_schema.statistics")
      .where({ table_schema: database, table_name: table })
      .groupBy("index_name", "non_unique", "index_type")
      .select({
        name: "index_name",
        algorithm: "index_type",
        unique: this.knex.raw("IF(non_unique = 0, 1, 0)"),
        columns: this.knex.raw(
          "GROUP_CONCAT(column_name ORDER BY seq_in_index ASC)"
        ),
      });

    return rows.map((row) => ({ ...row, columns: row.columns.split(",") }));
  }

  async _rawListForeignKeys(table) {
    const database = this.knex.client.database();

    return this.knex("information_schema.key_column_usage")
      .where({ referenced_table_schema: database, table_name: table })
      .select({
        name: "constraint_name",
        from: "column_name",
        to: "referenced_column_name",
        table: "referenced_table_name",
      });
  }

  async _rawListDatabases() {
    return this.knex("information_schema.schemata").pluck("SCHEMA_NAME");
  }
}

module.exports = MysqlSchemaBuilder;
