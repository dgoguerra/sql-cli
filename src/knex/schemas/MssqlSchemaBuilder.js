const BaseSchemaBuilder = require("./BaseSchemaBuilder");

class MssqlSchemaBuilder extends BaseSchemaBuilder {
  async getPrimaryKey(table) {
    return this.knex("information_schema.key_column_usage")
      .whereRaw(
        `OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1`
      )
      .andWhereRaw("TABLE_SCHEMA = SCHEMA_NAME()")
      .andWhere({ table_name: table })
      .pluck("column_name");
  }

  async _rawListColumns(table) {
    const database = this.knex.client.database();

    return this.knex("information_schema.columns")
      .where({
        table_schema: this.knex.raw("schema_name()"),
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
    return this.knex.raw(
      `SELECT
        s.table_name AS [table],
        MAX(p.rows) AS [rows],
        SUM(a.used_pages) * 8 * 1000 AS [bytes]
      FROM information_schema.tables [s]
        LEFT JOIN sys.tables [t]
          ON s.table_name = t.Name AND TABLE_TYPE = 'BASE TABLE'
        LEFT JOIN sys.indexes [i] ON t.object_id = i.object_id
        LEFT JOIN sys.partitions [p]
          ON i.object_id = p.object_id AND i.index_id = p.index_id
        LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
      WHERE s.table_schema != 'sys'
        AND (i.object_id is null OR i.object_id > 255)
      GROUP BY s.table_name, t.Name
      ORDER BY t.Name;`
    );
  }

  async _rawListIndexes(table) {
    const rows = await this.knex.raw(
      `SELECT i.name [name],
        i.is_unique [unique],
        i.type_desc [algorithm],
        STRING_AGG(ac.Name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) [columns]
      FROM sys.tables [t]
        INNER JOIN sys.indexes [i] ON t.object_id = i.object_id
        INNER JOIN sys.index_columns [ic]
          ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.all_columns [ac]
          ON ic.object_id = ac.object_id AND ic.column_id = ac.column_id
      WHERE t.name = '${table}' AND SCHEMA_NAME(t.schema_id) = SCHEMA_NAME()
      GROUP BY i.name, i.is_unique, i.type_desc`
    );

    return rows.map((row) => ({ ...row, columns: row.columns.split(",") }));
  }

  async _rawListForeignKeys(table) {
    return this.knex.raw(
      `SELECT
          fk.name AS [name],
          object_name (fk.referenced_object_id) AS [table],
          fromcol.name AS [from],
          tocol.name AS [to]
        FROM sys.foreign_keys AS fk
          JOIN sys.foreign_key_columns AS fkc
            ON fk.object_id = fkc.constraint_object_id
          JOIN sys.columns AS fromcol
            ON fkc.parent_object_id = fromcol.object_id
            AND fkc.parent_column_id = fromcol.column_id
          JOIN sys.columns AS tocol
            ON fkc.referenced_object_id = tocol.object_id
            AND fkc.referenced_column_id = tocol.column_id
        WHERE fk.parent_object_id = OBJECT_ID(SCHEMA_NAME() + '.${table}')`
    );
  }

  async _rawListDatabases() {
    return this.knex("master.sys.databases").pluck("name");
  }
}

module.exports = MssqlSchemaBuilder;
