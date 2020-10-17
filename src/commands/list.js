const _ = require("lodash");
const chalk = require("chalk");
const prettyBytes = require("pretty-bytes");
const table = require("../table");
const CliApp = require("../CliApp");

module.exports = {
  command: "list <conn>",
  aliases: ["ls"],
  description: "List tables",
  handler: async (argv) => {
    const lib = await CliApp.initLib(argv.conn, argv);
    const tables = await lib.schema.listTables();

    const formatted = _.sortBy(tables, [
      (row) => -row.bytes,
      (row) => row.table,
    ]).map((row) => ({ ...row, bytes: row.prettyBytes }));

    if (tables.length) {
      console.log(table(formatted, { headers: ["table", "rows", "bytes"] }));

      const totalBytes = tables.reduce((acc, row) => acc + (row.bytes || 0), 0);
      console.log("");
      console.log(
        chalk.grey(`(${prettyBytes(totalBytes)} in ${tables.length} tables)`)
      );
    } else {
      console.log("There are no tables in the connection");
    }

    await lib.destroy();
  },
};
