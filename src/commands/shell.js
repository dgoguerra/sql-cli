const CliApp = require("../CliApp");
const SqlRepl = require("../SqlRepl");

module.exports = {
  command: "shell <conn> [query]",
  aliases: ["sh"],
  description: "Run REPL shell",
  handler: async (argv) => {
    const lib = await CliApp.initLib(argv.conn, argv);

    // Check db connection before dropping the user to the shell,
    // to avoid waiting until a query is run to know that the
    // connection is invalid.
    try {
      await lib.checkConnection();
    } catch (err) {
      return CliApp.error(err.message);
    }

    await new SqlRepl(lib, { input: argv.query }).run();
    await lib.destroy();
  },
};
