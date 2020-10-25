const _ = require("lodash");
const prettyBytes = require("pretty-bytes");
const writer = require("flush-write-stream");
const { stringifyConn } = require("./connUtils");
const { chunk, runPipeline } = require("./streamUtils");

const KNEX_TYPES_MAP = {
  text: ["nvarchar(-1)", "longtext"],
  string: ["char", "character", "varchar", "nvarchar", "character varying"],
  boolean: ["tinyint"],
  integer: ["int"],
  bigInteger: ["bigint"],
  datetime: ["datetime2"],
  decimal: ["money", "numeric"],
  timestamp: ["timestamp with time zone"],
};

const hydrateKnex = (knex) => {
  // Methods to extend on Knex
  const KNEX_METHODS = {
    getUri() {
      return getUri(knex);
    },
  };

  // Methods to extend on Knex's QueryBuilder
  const QUERY_METHODS = {
    async countRows() {
      const [row] = await this.count({ count: "*" });
      return Number(row.count);
    },
    async getPrimaryKey() {
      return getPrimaryKey(knex, this._single.table);
    },
  };

  // Methods to extend on Knex's SchemaBuilder
  const SCHEMA_METHODS = {
    listTables() {
      return listTables(knex);
    },
    listColumns(table) {
      return listColumns(knex, table);
    },
    listIndexes(table) {
      return listIndexes(knex, table);
    },
    listForeignKeys(table) {
      return listForeignKeys(knex, table);
    },
    listDatabases() {
      return listDatabases(knex);
    },
    async getSchema() {
      const tables = await Promise.all(
        (await this.listTables()).map(async (table) => {
          const columns = await listColumns(knex, table.table);
          const indexes = await listIndexes(knex, table.table);
          return { ...table, columns, indexes };
        })
      );
      return tables.reduce((obj, table) => {
        obj[table.table] = table;
        return obj;
      }, {});
    },
  };

  // Proxy the created knex instance to use our custom methods
  return new Proxy(knex, {
    apply: (target, thisArg, argArray) => {
      const queryBuilder = target.apply(thisArg, argArray);
      return new Proxy(queryBuilder, {
        get: (target, prop) =>
          prop in QUERY_METHODS ? QUERY_METHODS[prop] : target[prop],
      });
    },
    get: (target, prop) => {
      if (prop in KNEX_METHODS) {
        return KNEX_METHODS[prop];
      }

      if (prop === "schema") {
        const schemaBuilder = target[prop];
        return new Proxy(schemaBuilder, {
          get: (target, prop) =>
            prop in SCHEMA_METHODS ? SCHEMA_METHODS[prop] : target[prop],
        });
      }

      return target[prop];
    },
  });
};

const getUri = (knex) => {
  const { client, connection: conn } = knex.client.config;
  return stringifyConn({
    protocol: client,
    host: conn.server || conn.host,
    ...conn,
  });
};

const listColumns = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();
  const foreignKeys = _.keyBy(await listForeignKeys(knex, table), "from");

  const formatRows = async (rows) =>
    rows.map((row) => {
      const foreign = foreignKeys[row.name];
      return {
        ...row,
        nullable: row.nullable === "YES" || row.nullable == 1,
        fullType: toFullType(row.type, row),
        default: cleanDefault(row.default),
        foreign: foreign ? `${foreign.table}.${foreign.to}` : null,
      };
    });

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

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.columns")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        name: "column_name",
        nullable: "is_nullable",
        type: "data_type",
        default: "column_default",
        maxLength: "character_maximum_length",
        unsigned: knex.raw(`instr(column_type, "unsigned")`),
        precision: "numeric_precision",
        scale: "numeric_scale",
      })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_PG") {
    return knex("information_schema.columns")
      .where({
        table_schema: knex.raw("current_schema()"),
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
      })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_MSSQL") {
    return knex("information_schema.columns")
      .where({
        table_schema: knex.raw("schema_name()"),
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
      })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_BigQuery") {
    // Note: BigQuery table names are case sensitive
    return knex("INFORMATION_SCHEMA.COLUMNS")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        column: "column_name",
        nullable: "is_nullable",
        type: "data_type",
      })
      .then((rows) => formatRows(rows));
  }

  // Fallback to knex's columnInfo()
  return knex(table)
    .columnInfo()
    .then((cols) =>
      Object.keys(cols).map((name) => {
        const { defaultValue, ...rest } = cols[name];
        return { name, default: defaultValue, ...rest };
      })
    )
    .then((rows) => formatRows(rows));
};

const listDatabases = async (knex) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const formatRows = (rows) =>
    rows.map((row) => ({
      database: row.database,
      current: database === row.database || null,
    }));

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.schemata")
      .select({ database: "schema_name" })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_MSSQL") {
    return knex("master.sys.databases")
      .select({ database: "name" })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_PG") {
    return knex("pg_database")
      .select({ database: "datname" })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_SQLite3") {
    return knex(knex.raw(`pragma_database_list`))
      .select({ database: "name" })
      .then((rows) => formatRows(rows));
  }

  return [];
};

// Snippet based on: https://github.com/knex/knex/issues/360#issuecomment-406483016
const listTables = async (knex) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const toNumberOrNull = (val) => (val || val === 0 ? Number(val) : null);

  const formatRows = (rows) =>
    rows.map(({ table, bytes, rows }) => {
      bytes = toNumberOrNull(bytes);
      rows = toNumberOrNull(rows);
      return {
        bytes,
        rows,
        table,
        prettyBytes: bytes !== null ? prettyBytes(bytes) : null,
      };
    });

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.tables")
      .where({ table_schema: database })
      .orderBy("table_name")
      .select({
        table: "table_name",
        bytes: knex.raw("data_length + index_length"),
        rows: "table_rows",
      })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_PG") {
    return knex
      .raw(
        `SELECT c.relname AS table,
          c.reltuples AS rows,
          pg_total_relation_size(c.oid) AS bytes
        FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname = current_schema()
          AND n.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY c.relname`
      )
      .then(({ rows }) => formatRows(rows));
  }

  if (client === "Client_SQLite3") {
    // Using dbstat virtual table. Requires sqlite3 to be compiled
    // with SQLITE_ENABLE_DBSTAT_VTAB=1, which is already available
    // in the precompiled binaries since v4.3 of sqlite3. See:
    // https://github.com/mapbox/node-sqlite3/issues/1279
    return knex("dbstat as s")
      .join("sqlite_master as t", "s.name", "=", "t.name")
      .whereRaw("s.name not like 'sqlite_%' and t.type = 'table'")
      .orderBy("s.name")
      .select({
        table: "s.name",
        rows: knex.raw("SUM(s.ncell)"),
        bytes: knex.raw("SUM(s.pgsize)"),
      })
      .groupBy("s.name")
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_MSSQL") {
    return knex
      .raw(
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
      )
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_Oracle" || client === "Client_Oracledb") {
    return knex("user_tables")
      .select({ table: "table_name" })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_BigQuery") {
    return knex("__TABLES__")
      .where({ dataset_id: database, type: 1 })
      .select({ table: "table_id", bytes: "size_bytes", rows: "row_count" })
      .then((rows) => formatRows(rows));
  }

  throw new Error(`Unexpected client '${client}', not implemented`);
};

const listForeignKeys = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const formatRows = (rows) =>
    rows.map((row) => ({
      name: row.name || null,
      from: row.from,
      to: row.to,
      table: row.table,
    }));

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.key_column_usage")
      .where({ referenced_table_schema: database, table_name: table })
      .select({
        name: "constraint_name",
        from: "column_name",
        to: "referenced_column_name",
        table: "referenced_table_name",
      })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_PG") {
    return knex
      .raw(
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
      )
      .then(({ rows }) => formatRows(rows));
  }

  if (client === "Client_MSSQL") {
    return knex
      .raw(
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
      )
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_SQLite3") {
    return knex(knex.raw(`pragma_foreign_key_list('${table}')`)).then((rows) =>
      formatRows(rows)
    );
  }

  return [];
};

const listIndexes = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const extractColsFromSql = (sql) => {
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
  };

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.statistics")
      .where({ table_schema: database, table_name: table })
      .groupBy("index_name", "non_unique", "index_type")
      .select({
        name: "index_name",
        algorithm: "index_type",
        unique: knex.raw("IF(non_unique = 0, 1, 0)"),
        columns: knex.raw(
          "GROUP_CONCAT(column_name ORDER BY seq_in_index ASC)"
        ),
      })
      .then((rows) =>
        rows.map((row) => ({
          name: row.name,
          unique: !!row.unique,
          algorithm: row.algorithm,
          columns: row.columns.split(","),
        }))
      );
  }

  if (client === "Client_SQLite3") {
    // Avoid rows without sql, to ignore any autoindexes created
    // by sqlite on the table.
    return knex("sqlite_master")
      .whereNotNull("sql")
      .andWhere({ type: "index", tbl_name: table })
      .then((rows) =>
        rows.map((row) => ({
          name: row.name,
          algorithm: "unknown",
          unique: row.sql.startsWith("CREATE UNIQUE "),
          columns: extractColsFromSql(row.sql),
        }))
      );
  }

  if (client === "Client_MSSQL") {
    return knex
      .raw(
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
      )
      .then((rows) =>
        rows.map((row) => ({
          name: row.name,
          unique: !!row.unique,
          algorithm: row.algorithm,
          columns: row.columns.split(","),
        }))
      );
  }

  if (client === "Client_PG") {
    return knex
      .raw(
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
      )
      .then(({ rows }) =>
        rows.map((row) => ({
          name: row.name,
          unique: !!row.unique,
          algorithm: row.algorithm,
          columns: extractColsFromSql(row.sql),
        }))
      );
  }

  return [];
};

const toKnexType = (type, opts = {}) => {
  const fullType = toFullType(type, opts);

  const findType = (type) =>
    _.findKey(KNEX_TYPES_MAP, (val, key) => key === type || val.includes(type));

  return findType(fullType) || findType(type) || null;
};

const toFullType = (
  type,
  { unsigned = null, precision = null, scale = null, maxLength = null } = {}
) => {
  let fullType = type;
  if (unsigned) {
    fullType += " unsigned";
  }
  if (precision && scale) {
    fullType += `(${precision},${scale})`;
  }
  if (maxLength) {
    fullType += `(${maxLength})`;
  }
  return fullType;
};

const getPrimaryKey = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const formatRows = (rows) => rows.map((row) => row.column);

  if (client === "Client_SQLite3") {
    return knex(knex.raw(`pragma_table_info('${table}')`))
      .where("pk", "!=", 0)
      .select({ column: "name" })
      .orderBy("pk")
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_PG") {
    return knex
      .raw(
        `SELECT pg_attribute.attname as column
        FROM pg_index, pg_class, pg_attribute, pg_namespace
        WHERE pg_class.oid = '${table}'::regclass
          AND indrelid = pg_class.oid
          AND nspname = current_schema()
          AND pg_class.relnamespace = pg_namespace.oid
          AND pg_attribute.attrelid = pg_class.oid
          AND pg_attribute.attnum = any(pg_index.indkey)
          AND indisprimary`
      )
      .then(({ rows }) => formatRows(rows));
  }

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return knex("information_schema.statistics")
      .where({
        table_schema: database,
        table_name: table,
        index_name: "PRIMARY",
      })
      .select({ column: "column_name" })
      .then((rows) => formatRows(rows));
  }

  if (client === "Client_MSSQL") {
    return knex("information_schema.key_column_usage")
      .whereRaw(
        `OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1`
      )
      .andWhereRaw("TABLE_SCHEMA = SCHEMA_NAME()")
      .andWhere({ table_name: table })
      .select({ column: "column_name" })
      .then((rows) => formatRows(rows));
  }

  return [];
};

const streamInsert = async (knex, table, stream) => {
  const client = knex.client.constructor.name;

  if (client === "Client_MSSQL") {
    return streamInsertMssql(knex, table, stream);
  }

  return streamInsertGeneric(knex, table, stream);
};

const streamInsertGeneric = async (knex, table, stream) => {
  const client = knex.client.constructor.name;

  // By default SQLite has a rather small maximum of SQL variables per
  // prepared statement. Calculate maximum chunks size we should use
  // for the table we want to insert.
  const chunkSize =
    client === "Client_SQLite3"
      ? await calculateSqliteChunkSize(knex, table)
      : 500;

  await runPipeline(
    stream,
    chunk(chunkSize),
    writer.obj((rows, enc, next) =>
      knex(table)
        .insert(rows)
        .then(() => next())
        .catch((err) => next(err))
    )
  );
};

const calculateSqliteChunkSize = async (knex, table) => {
  // SQLite has a maximum of 999 SQL variables per prepared statement.
  const MAX_SQLITE_VARS = 999;

  // Calculate the size of each chunk to bulk insert based on the
  // table's number of columns, to make sure if all columns were
  // inserted for all rows, we would still be below that hard limit.
  // This avoids the error: "SQLITE_ERROR: too many SQL variables"
  // on tables with a large amount of columns.
  const columns = await knex(table).columnInfo();
  const chunkSize = MAX_SQLITE_VARS / Object.keys(columns).length;

  // Return chunk size casted to int
  return 0 | chunkSize;
};

const streamInsertMssql = async (knex, table, stream) => {
  const primaryKey = await getPrimaryKey(knex, table);
  const columns = await knex(table).columnInfo();

  await runPipeline(
    stream,
    chunk(),
    writer.obj((rows, enc, next) =>
      bulkMssqlInsert(knex, table, rows, { columns, primaryKey })
        .then(() => next())
        .catch((err) => next(err))
    )
  );
};

const bulkMssqlInsert = async (
  knex,
  table,
  rows,
  { columns = {}, primaryKey = null } = {}
) => {
  // Get underlying MSSQL client. See: https://www.npmjs.com/package/mssql
  const mssql = knex.client.driver;
  const columnKeys = Object.keys(columns);

  const customTypes = {
    DECIMAL: () => mssql.DECIMAL(32, 16),
    TEXT: () => mssql.NVARCHAR(mssql.MAX),
  };

  const msTable = new mssql.Table(table);
  msTable.create = false;

  columnKeys.forEach((colKey) => {
    const { type, nullable } = columns[colKey];
    const msTypeName = type.toUpperCase();

    const msType = customTypes[msTypeName] || mssql[msTypeName];
    if (!msType) {
      throw new Error(
        `Unknown MSSQL column type '${msTypeName}'. A valid ` +
          `data type should be used. See: ` +
          `https://www.npmjs.com/package/mssql#data-types`
      );
    }

    msTable.columns.add(colKey, msType, {
      primary: primaryKey.includes(colKey),
      length: Infinity,
      nullable,
    });
  });

  rows.forEach((row) => {
    msTable.rows.add(...columnKeys.map((col) => row[col]));
  });

  const mssqlConn = await knex.client.acquireConnection();

  try {
    await new mssql.Request(mssqlConn).bulk(msTable, {
      keepNulls: true,
    });
  } catch (err) {
    await knex.client.releaseConnection(mssqlConn);
    throw err;
  }

  await knex.client.releaseConnection(mssqlConn);
};

module.exports = {
  hydrateKnex,
  toKnexType,
  streamInsert,
};
