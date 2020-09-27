const table = require("../table");
const CliApp = require("../CliApp");
const { streamsDiff } = require("../streamUtils");
const { diffColumns, diffIndexes, diffSchemas } = require("../schemaDiff");

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
      .option("rows", {
        description: "Number of rows to diff. Only has effect with --data",
        type: "number",
        default: 20,
      }),
  handler: async (argv) => {
    const conn1 = CliApp.resolveConn(argv.table1, argv);
    const conn2 = CliApp.resolveConn(argv.table2, argv);

    const lib1 = await CliApp.initLib(conn1);
    const lib2 = await CliApp.initLib(conn2);

    if (conn1.table && conn2.table && argv.data) {
      await runDiffTablesData(lib1, lib2, conn1.table, conn2.table, argv);
    } else if (conn1.table && conn2.table) {
      await runDiffTablesSchema(lib1, lib2, conn1.table, conn2.table, argv);
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
}

async function runDiffTablesData(lib1, lib2, table1, table2, argv) {
  if (!(await lib1.schema.hasTable(table1))) {
    CliApp.error(`Table '${table1}' not found in 'before' schema`);
  }
  if (!(await lib2.schema.hasTable(table2))) {
    CliApp.error(`Table '${table2}' not found in 'after' schema`);
  }

  console.log(`Diff of tables content (first ${argv.rows} rows):`);
  console.log("");

  const rows = await streamsDiff(
    lib1.knex(table1).limit(argv.rows).stream(),
    lib2.knex(table2).limit(argv.rows).stream(),
    { allRows: argv.all, allColumns: false }
  );

  console.log(rows.length ? table(rows) : "No table content changes");
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
}