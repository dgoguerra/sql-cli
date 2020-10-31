const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class OracleSchemaBuilder extends BaseSchemaBuilder {
  async _rawListTables() {
    return this.knex("user_tables").select({ table: "table_name" });
  }
}

module.exports = OracleSchemaBuilder;
