const _ = require("lodash");
const path = require("path");
const CliApp = require("../CliApp");
const ExcelBuilder = require("../ExcelBuilder");

module.exports = {
  command: "export <conn> [file]",
  description: "Export the connection's schema or data in XLSX",
  builder: (yargs) =>
    yargs
      .option("schema", {
        description: "Export the connection's schema",
        type: "boolean",
      })
      .option("data", {
        description: "Export the connection's data",
        type: "boolean",
      })
      .option("query", {
        description: "Export a custom query",
        type: "string",
      }),
  handler: async (argv) => {
    const lib = await CliApp.initLib(argv.conn, argv);

    if (!argv.schema && !argv.data && !argv.query) {
      CliApp.error("Provide either --schema, --data or --query=<sql>");
    }

    const builder = new ExcelBuilder();
    const filePath = path.resolve(
      argv.file || `${lib.buildConnSlug("export")}.xlsx`
    );

    if (argv.schema) {
      for (const table of await lib.schema.listTables()) {
        const columns = await lib.schema.listColumns(table.table);
        const rows = columns.map((col) => ({
          Column: col.name,
          Type: col.fullType,
          Nullable: col.nullable,
          Default: col.default,
          "Foreign Key": col.foreign,
        }));
        builder.addSheet(table.table, rows);
      }
    }

    if (argv.data) {
      for (const table of await lib.schema.listTables()) {
        builder.addSheet(table.table, await lib(table.table));
      }
    }

    if (argv.query) {
      builder.addSheet("Sheet1", await lib.raw(argv.query));
    }

    builder.writeFile(filePath);

    console.log(filePath);

    await lib.destroy();
  },
};
