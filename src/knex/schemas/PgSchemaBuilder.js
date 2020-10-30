const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class PgSchemaBuilder extends BaseSchemaBuilder {
  async getPrimaryKey(table) {
    const { rows } = await this.knex.raw(
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

    return this.knex("information_schema.columns")
      .where({
        table_schema: this.knex.raw("current_schema()"),
        table_catalog: database,
        table_name: table,
      })
      .orderBy("ordinal_position")
      .select({
        name: "column_name",
        nullable: "is_nullable",
        type: "data_type",
        default: "column_default",
        maxLength: "character_maximum_length",
        precision: "numeric_precision",
        scale: "numeric_scale",
      });
  }

  async _rawListTables() {
    const { rows } = await this.knex.raw(
      `SELECT c.relname AS table,
        c.reltuples AS rows,
        pg_total_relation_size(c.oid) AS bytes
      FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r'
        AND n.nspname = current_schema()
        AND n.nspname NOT IN ('information_schema', 'pg_catalog')
      ORDER BY c.relname`
    );
    return rows;
  }

  async _rawListIndexes(table) {
    const { rows } = await this.knex.raw(
      `SELECT
        ix.relname AS name,
        am.amname AS algorithm,
        indisunique AS unique,
        pg_get_indexdef(indexrelid) AS sql
      FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_class ix ON ix.oid = i.indexrelid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        JOIN pg_am AS am ON ix.relam = am.oid
      WHERE t.relname = '${table}' AND n.nspname = current_schema()`
    );

    return rows;
  }

  async _rawListForeignKeys(table) {
    const { rows } = await this.knex.raw(
      `SELECT
        tc.constraint_name AS name,
        kcu.column_name AS from,
        ccu.column_name AS to,
        ccu.table_name AS table
      FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          USING (constraint_schema, constraint_name, table_schema)
        JOIN information_schema.constraint_column_usage AS ccu
          USING (constraint_schema, constraint_name, table_schema)
      WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name='${table}'`
    );
    return rows;
  }

  async _rawListDatabases() {
    return this.knex("pg_database").pluck("datname");
  }
}

module.exports = PgSchemaBuilder;
