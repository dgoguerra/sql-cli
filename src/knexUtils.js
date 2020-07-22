const writer = require("flush-write-stream");
const { chunk, runPipeline } = require("./streamUtils");

const columnInfo = async (knex, table) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  if (client === "Client_BigQuery") {
    const results = await knex("INFORMATION_SCHEMA.COLUMNS")
      .where({ table_schema: database, table_name: table })
      .select({
        column: "column_name",
        nullable: "is_nullable",
        type: "data_type",
      });

    return _.transform(
      results,
      (acc, row) => {
        acc[row.column] = {
          nullable: row.nullable === "YES",
          type: row.type,
        };
      },
      {}
    );
  }

  return await knex(table).columnInfo();
};

// Snippet based on: https://github.com/knex/knex/issues/360#issuecomment-406483016
const listTables = async (knex) => {
  const client = knex.client.constructor.name;
  const database = knex.client.database();

  if (client === "Client_MySQL" || client === "Client_MySQL2") {
    return await knex("information_schema.tables")
      .where({ table_schema: database })
      .select({
        table: "table_name",
        bytes: knex.raw("data_length + index_length"),
        rows: "table_rows",
      });
  }

  if (client === "Client_PG") {
    return await knex("information_schema.tables")
      .where({
        table_schema: knex.raw("current_schema()"),
        table_catalog: database,
      })
      .select({ table: "table_name" });
  }

  if (client === "Client_SQLite3") {
    return await knex("sqlite_master")
      .where({ type: "table" })
      .select({ table: "name" });
  }

  if (client === "Client_MSSQL") {
    const rows = await knex.raw(`
      SELECT
        s.table_name AS [table],
        p.rows AS [rows],
        SUM(a.used_pages) * 8 AS [usedSpaceKB]
      FROM
        information_schema.tables [s]
        LEFT JOIN sys.tables [t] ON s.table_name = t.Name
          AND TABLE_TYPE = 'BASE TABLE'
        LEFT JOIN sys.indexes [i] ON t.object_id = i.object_id
        LEFT JOIN sys.partitions [p] ON i.object_id = p.object_id
          AND i.index_id = p.index_id
        LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
      WHERE
        s.table_schema != 'sys' AND (i.object_id is null OR i.object_id > 255)
      GROUP BY s.table_name, t.Name, p.Rows
      ORDER BY t.Name;
    `);

    return rows.map((row) => ({
      ...row,
      rows: Number(row.rows),
      bytes: Number(row.usedSpaceKB * 1000),
    }));
  }

  if (client === "Client_Oracle" || client === "Client_Oracledb") {
    return await knex("user_tables").select({ table: "table_name" });
  }

  if (client === "Client_BigQuery") {
    return await knex("__TABLES__")
      .where({ dataset_id: database, type: 1 })
      .select({ table: "table_id", bytes: "size_bytes", rows: "row_count" });
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

  if (client === "Client_MSSQL") {
    const rows = await knex.raw(`
      SELECT i.name [index],
        i.is_unique [unique],
        STRING_AGG(ac.Name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) [columns]
      FROM sys.tables [t]
        INNER JOIN sys.indexes [i] ON t.object_id = i.object_id
        INNER JOIN sys.index_columns [ic] ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.all_columns [ac] ON ic.object_id = ac.object_id AND ic.column_id = ac.column_id
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

const countRows = async (knex, table) => {
  const [row] = await knex(table).count({ count: "*" });
  return Number(row.count);
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
    datetime: "dateTime",
    datetime2: "dateTime", // mssql
    money: "decimal", // mssql
  };
  return TYPES_MAP[fullType] || TYPES_MAP[type] || type;
};

const findPrimaryKey = async (knex, table) => {
  const indexes = await listIndexes(knex, table);

  for (const index of indexes) {
    if (index.unique && index.columns.length === 1) {
      return index.columns[0];
    }
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
    writer.obj(async (rows, enc, next) => {
      await knex(table).insert(rows);
      next();
    })
  );
};

const streamInsertMssql = async (knex, table, stream) => {
  const columns = await knex(table).columnInfo();
  const primaryKey = await findPrimaryKey(knex, table);

  await runPipeline(
    stream,
    chunk(),
    writer.obj(async (rows, enc, next) => {
      await bulkMssqlInsert(knex, table, rows, { columns, primaryKey });
      next();
    })
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
  columnInfo,
  listTables,
  listIndexes,
  countRows,
  toKnexType,
  streamInsert,
};
