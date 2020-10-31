const _ = require("lodash");
const table = require("../table");
const CliApp = require("../CliApp");

module.exports = {
  command: "show <table>",
  description: "Show table structure",
  handler: async (argv) => {
    const conn = CliApp.resolveConn(argv.table, argv);

    if (!conn._table) {
      CliApp.error("No table was specified in the connection");
    }

    const lib = await CliApp.initLib(argv.table);

    const columns = await lib.schema.listColumns(conn._table);
    const indexes = await lib.schema.listIndexes(conn._table);

    const formatted = _.map(columns, (col) => ({
      column: col.name,
      type: col.fullType,
      nullable: col.nullable,
      default: col.default,
      "foreign key": col.foreign,
    }));

    console.log(table(formatted));
    console.log("");

    if (indexes.length) {
      const formatted = indexes.map((ind) => ({
        index: ind.name,
        algorithm: ind.algorithm,
        unique: ind.unique,
        columns: ind.columns,
      }));
      console.log(table(formatted));
    } else {
      console.log("There are no indexes in the table");
    }

    await lib.destroy();
  },
};
