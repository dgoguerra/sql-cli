const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class BigQuerySchemaBuilder extends BaseSchemaBuilder {
  async _rawListColumns(table) {
    const database = this.knex.client.database();

    // Note: BigQuery table names are case sensitive
    return this.knex("INFORMATION_SCHEMA.COLUMNS")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        name: "column_name",
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
