const table = require("../table");
const CliApp = require("../CliApp");
const { streamsDiff } = require("../streamUtils");
const { diffColumns, diffIndexes, diffSchemas } = require("../schemaDiff");
const chalk = require("chalk");

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
      .option("key", {
        description: "Key field to diff rows by. Only has effect with --data",
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

    if (table1 && table2 && argv.data) {
      await runDiffTablesData(lib1, lib2, table1, table2, argv);
    } else if (table1 && table2) {
      await runDiffTablesSchema(lib1, lib2, table1, table2, argv);
    } else {
      await runDiffSchemas(lib1, lib2, argv);
    }

    await lib1.destroy();
    await lib2.destroy();
  },
};

async function runDiffTablesSchema(lib1, lib2, table1, table2, argv) {
  if (!(await lib1.schema.hasTable(table1))) {
    CliApp.error(`Table '${table1}' not found in 'before' schema`);
  }
  if (!(await lib2.schema.hasTable(table2))) {
    CliApp.error(`Table '${table2}' not found in 'after' schema`);
  }

  const { columns, summary: colSummary } = diffColumns(
    await lib1(table1).columnInfo(),
    await lib2(table2).columnInfo(),
    { showSimilar: argv.all }
  );

  if (columns.length) {
    const formatted = columns.map((col) => ({
      column: col.displayColumn,
      type: col.displayType,
      nullable: col.displayNullable,
      default: col.displayDefault,
    }));
    console.log(table(formatted));
  }

  const {
    indexes,
    summary: indSummary,
  } = diffIndexes(
    await lib1.schema.listIndexes(table1),
    await lib2.schema.listIndexes(table2),
    { showSimilar: argv.all }
  );

  if (indexes.length) {
    if (columns.length) {
      console.log("");
    }

    const formatted = indexes.map((ind) => ({
      index: ind.displayIndex,
      algorithm: ind.displayAlgorithm,
      unique: ind.displayUnique,
      columns: ind.displayColumns,
    }));
    console.log(table(formatted));
  }

  if (argv.all || columns.length || indexes.length) {
    console.log("");
  }

  console.log(`Columns: ${colSummary}`);
  console.log(`Indexes: ${indSummary}`);

  if (
    !argv.all &&
    (colSummary.includes("(hidden)") || indSummary.includes("(hidden)"))
  ) {
    console.log(chalk.grey("Re-run with --all to show hidden rows"));
  }
}

async function runDiffTablesData(lib1, lib2, table1, table2, argv) {
  if (!(await lib1.schema.hasTable(table1))) {
    CliApp.error(`Table '${table1}' not found in 'before' schema`);
  }
  if (!(await lib2.schema.hasTable(table2))) {
    CliApp.error(`Table '${table2}' not found in 'after' schema`);
  }

  const first = argv.offset + 1;
  const last = first + argv.limit - 1;
  console.log(`Diff of tables content (rows ${first} to ${last}):`);
  console.log("");

  const streamRows = (knex, table) =>
    knex(table)
      .orderBy(argv.key)
      .limit(argv.limit)
      .offset(argv.offset)
      .select(knex.raw(argv.columns))
      .stream();

  const rows = await streamsDiff(
    streamRows(lib1, table1),
    streamRows(lib2, table2),
    { idKey: argv.key, allRows: argv.all, allColumns: false }
  );

  if (rows.length) {
    // Make sure the primary key is shown as first column of the table
    const headers = Object.keys(rows[0]).filter((key) => key !== argv.key);
    console.log(table(rows, { headers: [argv.key, ...headers] }));
  } else {
    console.log("No table content changes");
  }

  if (!argv.all && Number(argv.limit) !== rows.length) {
    console.log(chalk.grey("Re-run with --all to show rows without changes"));
  }
}

async function runDiffSchemas(lib1, lib2, argv) {
  const { tables, summary } = diffSchemas(
    await lib1.schema.tablesInfo(),
    await lib2.schema.tablesInfo(),
    { showSimilar: argv.all }
  );

  const formatted = tables.map((table) => ({
    table: table.displayTable,
    rows: table.displayRows,
    bytes: table.displayBytes,
    columns: table.colSummary,
    indexes: table.indSummary,
  }));

  if (!formatted.length) {
    console.log(`No tables with changes: ${summary}`);
    return;
  }

  console.log(table(formatted));
  console.log("");
  console.log(`Tables: ${summary}`);

  if (!argv.all && summary.includes("(hidden)")) {
    console.log(chalk.grey("Re-run with --all to show hidden rows"));
  }
}
