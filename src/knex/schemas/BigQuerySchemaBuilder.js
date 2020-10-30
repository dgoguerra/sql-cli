const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class BigQuerySchemaBuilder extends BaseSchemaBuilder {
  async getPrimaryKey(table) {
    const rows = await this.knex.raw(
      `SELECT pg_attribute.attname as column
      FROM pg_index, pg_class, pg_attribute, pg_namespace
      WHERE pg_class.oid = '${table}'::regclass
        AND indrelid = pg_class.oid
        AND nspname = current_schema()
        AND pg_class.relnamespace = pg_namespace.oid
        AND pg_attribute.attrelid = pg_class.oid
        AND pg_attribute.attnum = any(pg_index.indkey)
        AND indisprimary`
    );

    return rows.map((r) => r.column);
  }

  async _rawListColumns(table) {
    const database = this.knex.client.database();

    // Note: BigQuery table names are case sensitive
    return this.knex("INFORMATION_SCHEMA.COLUMNS")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        column: "column_name",
        nullable: "is_nullable",
        type: "data_type",
      });
  }

  async _rawListTables() {
    const database = this.knex.client.database();

    return this.knex("__TABLES__")
      .where({ dataset_id: database, type: 1 })
      .select({ table: "table_id", bytes: "size_bytes", rows: "row_count" });
  }
}

module.exports = BigQuerySchemaBuilder;
