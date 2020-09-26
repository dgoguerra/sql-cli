#!/usr/bin/env node

const cp = require("child_process");
const _ = require("lodash");
const Conf = require("conf");
const yargs = require("yargs");
const chalk = require("chalk");
const keytar = require("keytar");
const pkg = require("./package.json");
const debug = require("debug")("sql-cli");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const SqlLib = require("./src/SqlLib");
const SqlRepl = require("./src/SqlRepl");
const ExcelBuilder = require("./src/ExcelBuilder");
const { getExternalAliases } = require("./src/externalAliases");
const { diffColumns, diffIndexes, diffSchemas } = require("./src/schemaDiff");
const { resolveKnexConn, stringifyConn } = require("./src/resolveKnexConn");
const { streamsDiff } = require("./src/streamUtils");
const SqlDumper = require("./src/SqlDumper");

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
      .demandCommand();

    cli.command({
      command: "list <conn>",
      aliases: ["ls"],
      description: "List tables",
      handler: (argv) => this.listTables(argv),
    });

    cli.command({
      command: "show <table>",
      description: "Show table structure",
      handler: (argv) => this.showTable(argv),
    });

    cli.command({
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
      handler: (argv) => this.diffTablesOrSchemas(argv),
    });

    cli.command({
      command: "export <conn>",
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
      handler: (argv) => this.createXlsxExport(argv),
    });

    cli.command({
      command: "open <conn>",
      description: "Open in configured GUI (such as TablePlus)",
      handler: (argv) => this.openGui(argv),
    });

    cli.command({
      command: "shell <conn>",
      aliases: ["sh"],
      description: "Run REPL shell",
      handler: (argv) => this.runInteractiveShell(argv),
    });

    cli.command({
      command: "alias <action>",
      description: "Manage saved connection aliases",
      builder: (yargs) =>
        yargs
          .command({
            command: "list",
            aliases: ["ls"],
            description: "List existing aliases",
            handler: () => this.listAliases(),
          })
          .command({
            command: "add <alias> <conn>",
            description: "Add new alias",
            handler: (argv) => this.addAlias(argv),
          })
          .command({
            command: "remove <alias>",
            aliases: ["rm"],
            description: "Remove saved alias",
            handler: (argv) => this.removeAlias(argv),
          })
          .demandCommand(),
    });

    cli.command({
      command: "dump <action>",
      description: "Manage connection dumps",
      builder: (yargs) =>
        yargs
          .command({
            command: "create <conn> [name]",
            description: "Create a dump of the connection",
            handler: (argv) => this.createDump(argv),
          })
          .command({
            command: "load <conn> <dump>",
            description: "Load a dump to the connection",
            handler: (argv) => this.loadDump(argv),
          })
          .demandCommand(),
    });

    return cli;
  }

  async listTables(argv) {
    const lib = await this.initLib(argv.conn, argv);
    const tables = await lib.schema.listTables();

    const formatted = _.sortBy(tables, [
      (row) => -row.bytes,
      (row) => row.table,
    ]).map((row) => ({ ...row, bytes: row.prettyBytes }));

    console.log(table(formatted, { headers: ["table", "rows", "bytes"] }));

    const totalBytes = tables.reduce((acc, row) => acc + (row.bytes || 0), 0);
    console.log("");
    console.log(
      chalk.grey(`(${prettyBytes(totalBytes)} in ${tables.length} tables)`)
    );

    await lib.destroy();
  }

  async showTable(argv) {
    const conn = this.resolveConn(argv.table, argv);

    if (!conn.table) {
      this.error("No table was specified in the connection");
    }

    const lib = await this.initLib(conn);

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
  }

  async diffTablesOrSchemas(argv) {
    const conn1 = this.resolveConn(argv.table1, argv);
    const conn2 = this.resolveConn(argv.table2, argv);

    const lib1 = await this.initLib(conn1);
    const lib2 = await this.initLib(conn2);

    if (conn1.table && conn2.table && argv.data) {
      await this._diffTablesData(lib1, lib2, conn1.table, conn2.table, argv);
    } else if (conn1.table && conn2.table) {
      await this._diffTablesSchema(lib1, lib2, conn1.table, conn2.table, argv);
    } else {
      await this._diffSchemas(lib1, lib2, argv);
    }

    await lib1.destroy();
    await lib2.destroy();
  }

  async _diffTablesSchema(lib1, lib2, table1, table2, argv) {
    if (!(await lib1.schema.hasTable(table1))) {
      this.error(`Table '${table1}' not found in 'before' schema`);
    }
    if (!(await lib2.schema.hasTable(table2))) {
      this.error(`Table '${table2}' not found in 'after' schema`);
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

  async _diffTablesData(lib1, lib2, table1, table2, argv) {
    if (!(await lib1.schema.hasTable(table1))) {
      this.error(`Table '${table1}' not found in 'before' schema`);
    }
    if (!(await lib2.schema.hasTable(table2))) {
      this.error(`Table '${table2}' not found in 'after' schema`);
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

  async _diffSchemas(lib1, lib2, argv) {
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

  async createXlsxExport(argv) {
    const lib = await this.initLib(argv.conn, argv);

    if (!argv.schema && !argv.data && !argv.query) {
      this.error("Provide either --schema, --data or --query=<sql>");
    }

    const builder = new ExcelBuilder();
    const filePath = `${process.env.PWD}/${lib.buildConnSlug("export")}.xlsx`;

    if (argv.schema) {
      for (const table of await lib.schema.listTables()) {
        const schema = await lib(table.table).columnInfo();
        const rows = Object.keys(schema).map((key) => {
          const { fullType, nullable } = schema[key];
          return {
            Column: key,
            Type: fullType,
            Nullable: nullable,
          };
        });
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
  }

  async createDump(argv) {
    const lib = await this.initLib(argv.conn, argv);
    const dumper = new SqlDumper(lib);

    const dumpFile = await dumper.createDump(argv.name || null);
    console.log(dumpFile);
    await lib.destroy();
  }

  async loadDump(argv) {
    const lib = await this.initLib(argv.conn, argv);
    const dumper = new SqlDumper(lib);

    await dumper.loadDump(argv.dump);
    await lib.destroy();
  }

  async openGui(argv) {
    const toTablePlusConnUri = async (alias) => {
      // Convert the conn uri protocol to one understood by TablePlus
      const tablePlusProtos = {
        mssql: "sqlserver",
        pg: "postgres",
        mysql2: "mysql",
      };

      const { sshConf, conf } = this.resolveConn(alias);
      const { client, connection: conn } = conf;

      // Sqlite is opened directly by opening the file with the default
      // application for its file extension, without setting a protocol.
      if (client === "sqlite3") {
        return rest[0];
      }

      // Might need to resolve the password from the system's keychain
      if (!conn.password && this.aliasKeychains[alias]) {
        await this.resolveConnPassword(alias, conn);
      }

      // Rest of clients: build a connection uri with the protocol name
      // understood by TablePlus.
      let connUri = stringifyConn({
        // Convert the conn uri protocol to one understood by TablePlus
        protocol: tablePlusProtos[client] || client,
        path: conn.filename, // only set in SQLite
        host: conn.host || conn.server,
        ...conn,
        sshHost: sshConf && sshConf.host,
        sshPort: sshConf && sshConf.port,
        sshUser: sshConf && sshConf.user,
        sshPassword: sshConf && sshConf.password,
      });

      // If the connection has SSH configured but no SSH password,
      // then TablePlus needs to auth with the user's private key.
      if (sshConf && sshConf.host && !sshConf.password) {
        connUri += "?usePrivateKey=true";
      }

      return connUri;
    };

    const connUri = await toTablePlusConnUri(argv.conn);

    // Remove password from output
    console.log(`Opening ${connUri.replace(/:([^\/]+?)@/, "@")} ...`);

    // Open conn uri with default application, should be
    // TablePlus if installed.
    await new Promise((resolve, reject) =>
      cp.exec(`open ${connUri}`, (err) => (err ? reject(err) : resolve()))
    );
  }

  async runInteractiveShell(argv) {
    const lib = await this.initLib(argv.conn, argv);

    // Check db connection before dropping the user to the shell,
    // to avoid waiting until a query is run to know that the
    // connection is invalid.
    try {
      await lib.checkConnection();
    } catch (err) {
      return this.error(err.message);
    }

    await new SqlRepl(lib).run();
    await lib.destroy();
  }

  async listAliases() {
    const formatted = _.map(this.aliases, (conn, alias) => {
      const source = this.aliasSources[alias];
      return { alias: source ? `${alias} (${source})` : alias, conn };
    });
    console.log(table(formatted));
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

  async addAlias(argv) {
    if (this.aliases[argv.alias]) {
      this.error(`Alias '${argv.alias}' already exists`);
    }
    this.conf.set(`aliases.${argv.alias}`, argv.conn);

    console.log(`Created alias '${argv.alias}'`);
  }

  async removeAlias(argv) {
    if (!this.aliases[argv.alias]) {
      this.error(`Alias '${argv.alias}' not found`);
    }
    if (!this.conf.get(`aliases.${argv.alias}`)) {
      this.error(
        `Alias '${argv.alias}' is an imported alias, cannot be deleted`
      );
    }
    this.conf.delete(`aliases.${argv.alias}`);

    console.log(`Deleted alias '${argv.alias}'`);
  }

  async initLib(alias) {
    const { conf, sshConf } =
      typeof alias === "string" ? this.resolveConn(alias) : alias;

    if (
      conf.connection &&
      !conf.connection.password &&
      this.aliasKeychains[alias]
    ) {
      await this.resolveConnPassword(alias, conf.connection);
    }

    return await new SqlLib({ conf, sshConf }).connect();
  }

  async resolveConnPassword(alias, conn) {
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

  resolveConn(alias, argv = {}) {
    return resolveKnexConn(alias, {
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

new CliApp();
