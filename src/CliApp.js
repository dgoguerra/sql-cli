const Conf = require("conf");
const yargs = require("yargs");
const keytar = require("keytar");
const pkg = require("../package.json");
const debug = require("debug")("sql-cli");
const SqlLib = require("./SqlLib");
const SqlDumper = require("./SqlDumper");
const { getExternalAliases } = require("./externalAliases");
const { resolveConn } = require("./connUtils");

class CliApp {
  constructor() {
    this.conf = new Conf({
      projectName: pkg.name,
      // Allow setting a custom config directory, for testing
      cwd: process.env.SQL_CONF_DIR || null,
      defaults: { aliases: {} },
    });
    debug(`loading config from ${this.conf.path}`);

    this.aliases = {};
    this.aliasSources = {};
    this.aliasKeychains = {};

    // Load all saved aliases as an object indexed by the alias key.
    // Add also as aliases saved connections from TablePlus and Sequel Pro.
    this.loadInternalAliases();

    if (!process.env.SQL_NO_IMPORT_ALIASES) {
      this.loadExternalAliases();
    }
  }

  runYargs() {
    this.cli = this.buildYargs();
    this.argv = this.cli.argv;
  }

  buildYargs() {
    const cli = yargs
      .option("client", {
        alias: "c",
        description: "Knex client adapter",
        type: "string",
      })
      .help()
      .alias("h", "help")
      .version()
      .strict()
      .commandDir(`${__dirname}/commands`, { exclude: /\.spec\.js$/ })
      .demandCommand();

    return cli;
  }

  loadInternalAliases() {
    const internalAliases = this.conf.get("aliases");
    Object.keys(internalAliases).forEach((alias) => {
      this.aliases[alias] = internalAliases[alias];
    });
  }

  loadExternalAliases() {
    const allAliases = getExternalAliases();

    allAliases.forEach((item) => {
      const { source, alias, conn, keychain } = item;

      if (this.aliases[alias]) {
        debug(
          `ignoring external alias, already exists ` +
            `(alias=${alias}, source=${source})`
        );
        return;
      }

      this.aliases[alias] = conn;
      this.aliasSources[alias] = source;

      if (keychain) {
        this.aliasKeychains[alias] = keychain;
      }
    });
  }

  async initLib(connUri) {
    const conn = this.resolveConn(connUri);

    if (conn._alias && !conn.password && this.aliasKeychains[conn._alias]) {
      await this.lookupConnPassword(conn._alias, conn);
    }

    const lib = new SqlLib(conn);
    await lib.connect();

    // Connecting to a dumpfile as a sqlite table. Load it
    // before returning the connection.
    if (conn._alias && conn._alias.endsWith(".tgz")) {
      await new SqlDumper(lib).loadDump(conn._alias);
      return lib;
    }

    return lib;
  }

  async lookupConnPassword(alias, conn) {
    const { service, account } = this.aliasKeychains[alias];
    debug(
      `looking up password in system keychain (service=${service}, account=${account})`
    );
    const password = await keytar.getPassword(service, account);
    if (password) {
      debug("password found and attached to the connection");
      conn.password = password;
    } else {
      debug("password not found");
    }
  }

  resolveConn(connUri, argv = {}) {
    // If the connection is a path to a .tgz file, assume its a dump file
    // being passed instead of a connection.
    const matches = connUri.match(/(^.+\.tgz)\/?([^\/]+)?$/);
    if (matches && matches[1].endsWith(".tgz")) {
      const [_, _alias, _table] = matches;
      return { protocol: "sqlite3", filename: ":memory:", _table, _alias };
    }

    return resolveConn(connUri, {
      client: argv.client,
      aliases: this.aliases,
    });
  }

  error(message) {
    this.cli.showHelp();
    console.error(`\nError: ${message}\n`);
    process.exit(1);
  }
}

// Expose a default CliApp instance as a singleton
module.exports = new CliApp();
