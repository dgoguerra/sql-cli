const CliApp = require("../CliApp");
const SqlDumper = require("../SqlDumper");

module.exports = {
  command: "dump <action>",
  description: "Manage connection dumps",
  builder: (yargs) =>
    yargs
      .command({
        command: "create <conn> [name]",
        description: "Create a dump of the connection",
        handler: (argv) => createDump(argv),
      })
      .command({
        command: "load <conn> <dump>",
        description: "Load a dump to the connection",
        handler: (argv) => loadDump(argv),
      })
      .demandCommand(),
};

async function createDump(argv) {
  const lib = await CliApp.initLib(argv.conn, argv);
  const dumper = new SqlDumper(lib);

  const dumpFile = await dumper.createDump(argv.name || null);
  console.log(dumpFile);
  await lib.destroy();
}

async function loadDump(argv) {
  const lib = await CliApp.initLib(argv.conn, argv);
  const dumper = new SqlDumper(lib);

  await dumper.loadDump(argv.dump);
  await lib.destroy();
}
