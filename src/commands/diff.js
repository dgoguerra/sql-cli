const _ = require("lodash");
const chalk = require("chalk");
const table = require("../table");
const CliApp = require("../CliApp");
const { diffArrays, defaultFormatter } = require("../diffUtils");

module.exports = {
  command: "diff <table1> <table2>",
  description: "Diff two schemas or tables",
  builder: (yargs) =>
    yargs
      .option("all", {
        description: "Show items without changes",
        type: "boolean",
      })
      .option("data", {
        description: "Diff the tables' data",
        type: "boolean",
      })
      .option("query", {
        description: "Diff the tables' output of a query",
        type: "string",
        default: null,
      })
      .option("key", {
        description:
          "Key field to diff rows by. Only has effect with --data and --query",
        type: "string",
        default: "id",
      })
      .option("columns", {
        description: "Columns to diff. Only has effect with --data",
        alias: ["col"],
        type: "string",
        default: "*",
      })
      .option("limit", {
        description: "Number of rows to diff. Only has effect with --data",
        type: "number",
        default: 20,
      })
      .option("offset", {
        description: "Starting row to diff. Only has effect with --data",
        type: "number",
        default: 0,
      }),
  handler: async (argv) => {
    const { _table: table1 } = CliApp.resolveConn(argv.table1, argv);
    const { _table: table2 } = CliApp.resolveConn(argv.table2, argv);

    const lib1 = await CliApp.initLib(argv.table1);
    const lib2 = await CliApp.initLib(argv.table2);

    if (table1 && !(await lib1.schema.hasTable(table1))) {
      CliApp.error(`Table '${table1}' not found in 'before' schema`);
    }
    if (table2 && !(await lib2.schema.hasTable(table2))) {
      CliApp.error(`Table '${table2}' not found in 'after' schema`);
    }

    if (argv.data || argv.query) {
      await runDiffData(lib1, lib2, table1, table2, argv);
    } else if (table1 && table2) {
      await runDiffTables(lib1, lib2, table1, table2, argv);
    } else {
      await runDiffSchemas(lib1, lib2, argv);
    }

    await lib1.destroy();
    await lib2.destroy();
  },
};

async function runDiffTables(lib1, lib2, table1, table2, argv) {
  const cols = diffArrays(
    await listColumns(lib1, table1),
    await listColumns(lib2, table2),
    { keyBy: "column", allRows: !!argv.all }
  );

  const indexes1 = await listIndexes(lib1, table1);
  const indexes2 = await listIndexes(lib2, table2);
  const inds = diffArrays(indexes1, indexes2, {
    keyBy: getIndexesKeyBy(indexes1, indexes2),
    allRows: !!argv.all,
  });

  if (cols.items.length) {
    console.log(table(cols.items));
  }

  if (inds.items.length) {
    if (cols.items.length) {
      console.log("");
    }
    console.log(table(inds.items));
  }

  if (cols.items.length || inds.items.length) {
    console.log("");
  }

  console.log(`Columns: ${cols.summary}`);
  console.log(`Indexes: ${inds.summary}`);

  if (
    !argv.all &&
    (cols.summary.includes("(hidden)") || inds.summary.includes("(hidden)"))
  ) {
    console.log(chalk.grey("Re-run with --all to show hidden rows"));
  }
}

async function runDiffData(lib1, lib2, table1, table2, argv) {
  const first = argv.offset + 1;
  const last = first + argv.limit - 1;
  console.log(`Diff of tables content (rows ${first} to ${last}):`);
  console.log("");

  const { items, summary } = await diffArrays(
    await listRows(lib1, table1, argv),
    await listRows(lib2, table2, argv),
    { keyBy: argv.key, allRows: !!argv.all, allColumns: false }
  );

  if (items.length) {
    // Make sure the primary key is shown as first column of the table
    const headers = Object.keys(items[0]).filter((key) => key !== argv.key);
    console.log(table(items, { headers: [argv.key, ...headers] }));
    console.log("");
  }

  console.log(`Rows: ${summary}`);

  if (!argv.all && summary.includes("(hidden)")) {
    console.log(chalk.grey("Re-run with --all to show hidden rows"));
  }
}

async function runDiffSchemas(lib1, lib2, argv) {
  const { items, summary } = diffArrays(
    await listTables(lib1),
    await listTables(lib2),
    {
      keyBy: "table",
      allRows: !!argv.all,
      formatter: (status, field, val1, val2) => {
        if (field === "columns") {
          return diffArrays(val1, val2, { keyBy: "column" }).summary;
        }
        if (field === "indexes") {
          const keyBy = getIndexesKeyBy(val1, val2);
          return diffArrays(val1, val2, { keyBy }).summary;
        }
        // Format normally the rest of the fields
        return defaultFormatter(status, field, val1, val2);
      },
    }
  );

  if (items.length) {
    console.log(table(items));
    console.log("");
  }

  console.log(`Tables: ${summary}`);

  if (!argv.all && summary.includes("(hidden)")) {
    console.log(chalk.grey("Re-run with --all to show hidden rows"));
  }
}

function getIndexesKeyBy(indexes1, indexes2) {
  const keyByName = "index";
  const keyByHash = (i) => `${i.algorithm}:${i.unique}:${i.columns}`;

  const countMerged = (items1, items2, keyBy) => {
    const keys = Object.keys({
      ..._.keyBy(items1, keyBy),
      ..._.keyBy(items2, keyBy),
    });
    return keys.length;
  };

  // Indexes autogenerated by some tools may be created with different
  // randomised names each time, which would appear as totally different
  // indexes if we group them by their name (the "index" field).
  //
  // We want to detect those cases and show the index as "changed". To
  // achieve this, we try to key all indexes both by name, and by hash,
  // and continue with whichever results in less different indexes
  // (which means more matches found between the indexes of both tables).
  const countByHash = countMerged(indexes1, indexes2, keyByHash);
  const countByName = countMerged(indexes1, indexes2, keyByName);

  return countByHash <= countByName ? keyByHash : keyByName;
}

async function listColumns(knex, table) {
  return formatInputColumns(await knex.schema.listColumns(table));
}

async function listIndexes(knex, table) {
  return formatInputIndexes(await knex.schema.listIndexes(table));
}

function listRows(knex, table, argv) {
  if (argv.query) {
    const client = knex.client.constructor.name;
    return (
      knex
        .raw(argv.query)
        // In postgres knex.raw() returns results inside 'rows'
        .then((results) => (client === "Client_PG" ? results.rows : results))
    );
  }

  return knex(table)
    .orderBy(argv.key)
    .limit(argv.limit)
    .offset(argv.offset)
    .select(knex.raw(argv.columns));
}

async function listTables(knex) {
  const tables = await knex.schema.getSchema();
  return _.map(tables, (table, key) => ({
    table: key,
    rows: table.rows,
    bytes: table.prettyBytes,
    columns: formatInputColumns(table.columns),
    indexes: formatInputIndexes(table.indexes),
  }));
}

const formatInputColumns = (columns) =>
  _.map(columns, (col) => ({
    column: col.name,
    type: col.fullType,
    nullable: col.nullable,
    default: col.default,
  }));

const formatInputIndexes = (indexes) =>
  _.map(indexes, (ind) => ({
    index: ind.name,
    algorithm: ind.algorithm,
    unique: ind.unique,
    columns: ind.columns,
  }));
