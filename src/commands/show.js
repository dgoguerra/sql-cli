const _ = require("lodash");
const table = require("../table");
const CliApp = require("../CliApp");

module.exports = {
  command: "show <table>",
  description: "Show table structure",
  handler: async (argv) => {
    const conn = CliApp.resolveConn(argv.table, argv);

    if (!conn.table) {
      CliApp.error("No table was specified in the connection");
    }

    const lib = await CliApp.initLib(conn);

    const columns = await lib(conn.table).columnInfo();
    const indexes = await lib.schema.listIndexes(conn.table);

    const formatted = _.map(columns, (val, key) => ({
      column: key,
      type: val.fullType,
      nullable: val.nullable,
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
      console.log("No indexes in table");
    }

    await lib.destroy();
  },
};