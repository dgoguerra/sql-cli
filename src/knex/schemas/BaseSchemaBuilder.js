const _ = require("lodash");
const prettyBytes = require("pretty-bytes");
const { toFullType } = require("../utils");

const isNumeric = (v) =>
  (typeof v === "number" || typeof v === "string") &&
  Number.isFinite(Number(v));

// Depending on the client, default values may be returned as a string
// wrapped by quotes and/or parenthesis. Ex:
// default integer 0 -> returned as "('0')"
// default string "str" -> returned as "'str'"
const cleanDefault = (val) => {
  if (typeof val !== "string" || isNumeric(val)) {
    return val;
  }
  val = val.replace(/^\((.*?)\)$/, "$1"); // remove parenthesis
  val = val.replace(/^'(.*?)'$/, "$1"); // remove ''
  val = val.replace(/^"(.*?)"$/, "$1"); // remove ""
  val = val.replace(/^'(.*?)'::text$/, "$1"); // remove 'string'::text syntax (postgres)
  return val;
};

class BaseSchemaBuilder {
  constructor(knex, builder) {
    this.knex = knex;
    this.builder = builder;
  }

  async getPrimaryKey() {
    return [];
  }

  async listColumns(table) {
    const foreignKeys = _.keyBy(await this.listForeignKeys(table), "from");

    return (await this._rawListColumns(table)).map((row) => {
      const foreign = foreignKeys[row.name];
      return {
        ...row,
        nullable: row.nullable === "YES" || row.nullable == 1,
        fullType: toFullType(row.type, row),
        default: cleanDefault(row.default),
        foreign: foreign ? `${foreign.table}.${foreign.to}` : null,
      };
    });
  }

  // Fallback to knex's columnInfo()
  async _rawListColumns(table) {
    const columns = await this.knex(table).columnInfo();
    return Object.keys(columns).map((name) => {
      const { defaultValue, ...rest } = columns[name];
      return { name, default: defaultValue, ...rest };
    });
  }

  async listTables() {
    const toNumberOrNull = (val) => (val || val === 0 ? Number(val) : null);

    return (await this._rawListTables()).map(({ table, bytes, rows }) => {
      bytes = toNumberOrNull(bytes);
      rows = toNumberOrNull(rows);
      const pretty = bytes !== null ? prettyBytes(bytes) : null;
      return { bytes, rows, table, prettyBytes: pretty };
    });
  }

  async _rawListTables() {
    return [];
  }

  async listIndexes(table) {
    return (await this._rawListIndexes(table)).map((row) => ({
      name: row.name,
      algorithm: row.algorithm || "unknown",
      unique: !!row.unique,
      columns: row.columns || this._extractColumnsFromIndexSql(row.sql),
    }));
  }

  async _rawListIndexes(table) {
    return [];
  }

  _extractColumnsFromIndexSql(sql) {
    const matches = sql.match(/\(([^\(]+)\)$/);
    if (!matches || !matches.length) {
      return [];
    }
    return matches[1].split(/, ?/).map((col) => {
      // If column name has any trailing text, ignore it. For example, in
      // postgres index columns may have modifieres like "varchar_pattern_ops":
      // CREATE INDEX index ON table USING btree (my_field varchar_pattern_ops)
      const [colName] = col.split(" ");
      return _.trim(colName, '`"');
    });
  }

  async listForeignKeys(table) {
    return (await this._rawListForeignKeys(table)).map((row) => ({
      name: row.name || null,
      from: row.from,
      to: row.to,
      table: row.table,
    }));
  }

  async _rawListForeignKeys(table) {
    return [];
  }

  async listDatabases() {
    const current = this.knex.client.database();

    return (await this._rawListDatabases()).map((database) => ({
      database,
      current: current === database || null,
    }));
  }

  async _rawListDatabases() {
    return [];
  }

  async getSchema() {
    const tables = await Promise.all(
      (await this.listTables()).map(async (table) => {
        const columns = await this.listColumns(table.table);
        const indexes = await this.listIndexes(table.table);
        return { ...table, columns, indexes };
      })
    );
    return _.keyBy(tables, "table");
  }
}

module.exports = BaseSchemaBuilder;
