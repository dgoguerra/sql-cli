const _ = require("lodash");
const table = require("../table");
const CliApp = require("../CliApp");

module.exports = {
  command: "alias <action>",
  description: "Manage saved connection aliases",
  builder: (yargs) =>
    yargs
      .command({
        command: "list",
        aliases: ["ls"],
        description: "List existing aliases",
        handler: () => listAliases(),
      })
      .command({
        command: "add <alias> <conn>",
        description: "Add new alias",
        handler: (argv) => addAlias(argv),
      })
      .command({
        command: "remove <alias>",
        aliases: ["rm"],
        description: "Remove saved alias",
        handler: (argv) => removeAlias(argv),
      })
      .demandCommand(),
};

function listAliases() {
  const formatted = _.map(CliApp.aliases, (conn, alias) => {
    const source = CliApp.aliasSources[alias];
    return { alias: source ? `${alias} (${source})` : alias, conn };
  });
  console.log(table(formatted));
}

function addAlias(argv) {
  if (CliApp.aliases[argv.alias]) {
    CliApp.error(`Alias '${argv.alias}' already exists`);
  }
  CliApp.conf.set(`aliases.${argv.alias}`, argv.conn);

  console.log(`Created alias '${argv.alias}'`);
}

function removeAlias(argv) {
  if (!CliApp.aliases[argv.alias]) {
    CliApp.error(`Alias '${argv.alias}' not found`);
  }
  if (!CliApp.conf.get(`aliases.${argv.alias}`)) {
    CliApp.error(
      `Alias '${argv.alias}' is an imported alias, cannot be deleted`
    );
  }
  CliApp.conf.delete(`aliases.${argv.alias}`);

  console.log(`Deleted alias '${argv.alias}'`);
}
