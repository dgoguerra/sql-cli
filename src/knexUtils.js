const prettyBytes = require("pretty-bytes");
const writer = require("flush-write-stream");
const { chunk, runPipeline } = require("./streamUtils");

const hydrateKnex = (knex) => {
  // Methods to overwrite or create over Knex
  const KNEX_METHODS = {
    getUri() {
      return getUri(knex);
    },
  };

  // Methods to overwrite or create over Knex's QueryBuilder
  const QUERY_METHODS = {
    async columnInfo(column = null) {
      const columns = await getColumns(knex, this._single.table);
      return column ? columns[column] : columns;
    },
    async countRows() {
      const [row] = await this.count({ count: "*" });
      return Number(row.count);
    },
    async getPrimaryKey() {
      return getPrimaryKey(knex, this._single.table);
    },
  };

  // Methods to overwrite or create over Knex's SchemaBuilder
  const SCHEMA_METHODS = {
    listTables() {
      return listTables(knex);
    },
    listIndexes(table) {
      return listIndexes(knex, table);
    },
    async tablesInfo() {
      const tables = await Promise.all(
        (await this.listTables()).map(async (table) => {
          const columns = await getColumns(knex, table.table);
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
  if (client === "sqlite3") {
    return `${client}://${conn.filename}`;
  }
  const host = conn.server || conn.host;
  return `${client}://${conn.user}:${conn.password}@${host}:${conn.port}/${conn.database}`;
};

const getColumns = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  const _defaultGetColumns = (where = {}) => {
    return knex("information_schema.columns")
      .where(where)
      .orderBy("ordinal_position")
      .select({
        column: "column_name",
        nullable: "is_nullable",
        type: "data_type",
        defaultValue: "column_default",
        maxLength: "character_maximum_length",
      })
      .then((rows) => _defaultFormatResults(rows));
  };

  const _defaultFormatResults = (rows) => {
    const columns = {};
    rows.forEach((row) => {
      columns[row.column] = {
        nullable: row.nullable === "YES",
        type: row.type,
        defaultValue: row.defaultValue,
        maxLength: row.maxLength,
      };
    });
    return columns;
  };

  let results = {};

  // If possible, query manually instead of using knex(table).columnInfo().
  // The reason for this is to ensure ordering of the resulting columns
  // (the keys creation order should be kept in the returned object).
  // This expected ordering will facilitate testing.
  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    results = await _defaultGetColumns({
      table_schema: database,
      table_name: table,
    });
  } else if (client === "Client_PG") {
    results = await _defaultGetColumns({
      table_schema: knex.raw("current_schema()"),
      table_catalog: database,
      table_name: table,
    });
  } else if (client === "Client_MSSQL") {
    results = await _defaultGetColumns({
      table_schema: knex.raw("schema_name()"),
      table_catalog: database,
      table_name: table,
    });
  } else if (client === "Client_BigQuery") {
    // BigQuery table names are case sensitive
    results = await knex("INFORMATION_SCHEMA.COLUMNS")
      .where({ table_schema: database, table_name: table })
      .orderBy("ordinal_position")
      .select({
        column: "column_name",
        nullable: "is_nullable",
        type: "data_type",
      })
      .then((rows) => _defaultFormatResults(rows));
  } else {
    // Fallback to knex's columnInfo()
    results = await knex(table).columnInfo();
  }

  // Calculate extra fullType property of each column as helper
  for (const key in results) {
    const { type, maxLength } = results[key];
    results[key].fullType = maxLength ? `${type}(${maxLength})` : type;
  }

  return results;
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
          c.reltuples AS rows
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

const listIndexes = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    const rows = await knex("information_schema.statistics")
      .where({ table_schema: database, table_name: table })
      .groupBy("name", "non_unique")
      .select({
        name: "index_name",
        unique: knex.raw("IF(non_unique = 0, 1, 0)"),
        columns: knex.raw(
          "GROUP_CONCAT(column_name ORDER BY seq_in_index ASC)"
        ),
      });
    return rows.map((row) => ({
      ...row,
      unique: !!row.unique,
      columns: row.columns.split(","),
    }));
  }

  if (client === "Client_SQLite3") {
    const extractColumns = (sql) => {
      const matches = sql.match(/\(\`(.+)\`\)$/);
      if (!matches.length) {
        return [];
      }
      return matches[1].split("`, `");
    };

    return knex("sqlite_master")
      .where({ type: "index", tbl_name: table })
      .then((rows) =>
        rows.map((row) => ({
          name: row.name,
          unique: row.sql.startsWith("CREATE UNIQUE "),
          columns: extractColumns(row.sql),
        }))
      );
  }

  if (client === "Client_MSSQL") {
    const rows = await knex.raw(`
      SELECT i.name [index],
        i.is_unique [unique],
        STRING_AGG(ac.Name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) [columns]
      FROM sys.tables [t]
        INNER JOIN sys.indexes [i] ON t.object_id = i.object_id
        INNER JOIN sys.index_columns [ic]
          ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.all_columns [ac]
          ON ic.object_id = ac.object_id AND ic.column_id = ac.column_id
      WHERE t.name = '${table}' AND SCHEMA_NAME(t.schema_id) = SCHEMA_NAME()
      GROUP BY i.name, i.is_unique;
    `);
    return rows.map((row) => ({
      ...row,
      unique: !!row.unique,
      columns: row.columns.split(","),
    }));
  }

  return [];
};

const toKnexType = (type, maxLength = null) => {
  const fullType = `${type}(${maxLength})`;

  const TYPES_MAP = {
    "nvarchar(-1)": "text", // mssql
    nvarchar: "string", // mssql
    varchar: "string",
    longtext: "text",
    tinyint: "boolean",
    int: "integer",
    bigint: "bigInteger",
    datetime: "timestamp",
    datetime2: "timestamp", // mssql
    money: "decimal", // mssql
    "timestamp with time zone": "timestamp", // postgres
    numeric: "decimal", // postgres
  };
  return TYPES_MAP[fullType] || TYPES_MAP[type] || type;
};

const getPrimaryKey = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  if (client === "Client_SQLite3") {
    const row = await knex(knex.raw(`pragma_table_info('${table}')`))
      .where({ pk: 1 })
      .first("name");
    return row ? row.name : null;
  }

  if (client === "Client_PG") {
    const { rows } = await knex.raw(`
      SELECT pg_attribute.attname
      FROM pg_index, pg_class, pg_attribute, pg_namespace
      WHERE pg_class.oid = '${table}'::regclass
        AND indrelid = pg_class.oid
        AND nspname = current_schema()
        AND pg_class.relnamespace = pg_namespace.oid
        AND pg_attribute.attrelid = pg_class.oid
        AND pg_attribute.attnum = any(pg_index.indkey)
        AND indisprimary
    `);
    return rows.length ? rows[0].attname : null;
  }

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    const row = await knex("information_schema.statistics")
      .where({
        table_schema: database,
        table_name: table,
        index_name: "PRIMARY",
      })
      .first({ column: "column_name" });
    return row ? row.column : null;
  }

  if (client === "Client_MSSQL") {
    const row = await knex("information_schema.key_column_usage")
      .whereRaw(
        `OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1`
      )
      .andWhereRaw("TABLE_SCHEMA = SCHEMA_NAME()")
      .andWhere({ table_name: table })
      .first({ column: "column_name" });
    return row ? row.column : null;
  }

  return null;
};

const streamInsert = async (knex, table, stream) => {
  const client = knex.client.constructor.name;

  if (client === "Client_MSSQL") {
    return streamInsertMssql(knex, table, stream);
  }

  return streamInsertGeneric(knex, table, stream);
};

const streamInsertGeneric = async (knex, table, stream) => {
  await runPipeline(
    stream,
    chunk(),
    writer.obj((rows, enc, next) =>
      knex(table)
        .insert(rows)
        .then(() => next())
        .catch((err) => next(err))
    )
  );
};

const streamInsertMssql = async (knex, table, stream) => {
  const columns = await knex(table).columnInfo();
  const primaryKey = await getPrimaryKey(knex, table);

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
      primary: colKey === primaryKey,
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
