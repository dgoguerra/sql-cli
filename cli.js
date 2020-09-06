#!/usr/bin/env node

const cp = require("child_process");
const _ = require("lodash");
const Conf = require("conf");
const yargs = require("yargs");
const chalk = require("chalk");
const pkg = require("./package.json");
const debug = require("debug")("sql-cli");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const Lib = require("./src/Lib");
const SqlRepl = require("./src/SqlRepl");
const ExcelBuilder = require("./src/ExcelBuilder");
const { resolveKnexConn, stringifyKnexConn } = require("./src/resolveKnexConn");
const { diffColumns, diffSchemas } = require("./src/schemaDiff");
const { streamsDiff } = require("./src/streamUtils");

class CliApp {
  constructor() {
    this.conf = new Conf({
      projectName: pkg.name,
      // Allow setting a custom config directory, for testing
      cwd: process.env.SQL_CONF_DIR || null,
      defaults: { aliases: {} },
    });
    debug(`loading config from ${this.conf.path}`);
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
    const tables = await lib.listTables();

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

    const formatted = _.map(
      await lib.getTableSchema(conn.table),
      (val, key) => ({
        column: key,
        type: val.fullType,
        nullable: val.nullable,
      })
    );

    console.log(table(formatted));

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
      await this._diffTablesSchema(lib1, lib2, conn1.table, conn2.table);
    } else {
      await this._diffSchemas(lib1, lib2);
    }

    await lib1.destroy();
    await lib2.destroy();
  }

  async _diffTablesSchema(lib1, lib2, table1, table2) {
    if (!(await lib1.tableExists(table1))) {
      this.error(`Table '${table1}' not found in 'before' schema`);
    }
    if (!(await lib2.tableExists(table2))) {
      this.error(`Table '${table2}' not found in 'after' schema`);
    }

    const { columns, summary } = diffColumns(
      await lib1.getTableSchema(table1),
      await lib2.getTableSchema(table2)
    );

    const formattedCols = columns
      .filter((col) => col.status !== "similar")
      .map((col) => ({
        column: col.displayColumn,
        type: col.displayType,
      }));

    if (!formattedCols.length) {
      console.log(`No schema changes: ${summary}`);
      return;
    }

    console.log(
      table(
        formattedCols,
        // Disable default rows formatting, since the fields
        // already have diff colors applied.
        { headers: ["column", "type"], format: (val) => val }
      )
    );
    console.log("");
    console.log(summary);
  }

  async _diffTablesData(lib1, lib2, table1, table2, argv) {
    if (!(await lib1.tableExists(table1))) {
      this.error(`Table '${table1}' not found in 'before' schema`);
    }
    if (!(await lib2.tableExists(table2))) {
      this.error(`Table '${table2}' not found in 'after' schema`);
    }

    console.log(`Diff of tables content (first ${argv.rows} rows):`);
    console.log("");

    const formattedRows = await streamsDiff(
      lib1.knex(table1).limit(argv.rows).stream(),
      lib2.knex(table2).limit(argv.rows).stream(),
      { allRows: false, allColumns: false }
    );

    if (!formattedRows.length) {
      console.log("No table content changes");
      return;
    }

    console.log(
      table(formattedRows, {
        headers: Object.keys(formattedRows[0]),
        format: (val) => val,
      })
    );
  }

  async _diffSchemas(lib1, lib2) {
    const tables = diffSchemas(
      await lib1.getDatabaseSchema(),
      await lib2.getDatabaseSchema()
    );

    const formattedTables = tables
      .filter((col) => col.status !== "similar")
      .map((table) => ({
        table: table.displayTable,
        rows: table.displayRows,
        bytes: table.displayBytes,
        columns: table.displaySummary,
      }));

    if (!formattedTables.length) {
      console.log("No tables with changes");
      return;
    }

    console.log(
      table(formattedTables, {
        headers: ["table", "rows", "bytes", "columns"],
        // Disable default rows formatting, since the fields
        // already have diff colors applied.
        format: (val) => val,
      })
    );
  }

  async createXlsxExport(argv) {
    const lib = await this.initLib(argv.conn, argv);

    if (!argv.schema && !argv.data && !argv.query) {
      this.error("Provide either --schema, --data or --query=<sql>");
    }

    const builder = new ExcelBuilder();
    const filePath = `${process.env.PWD}/${lib.buildConnSlug("export")}.xlsx`;

    if (argv.schema) {
      for (const table of await lib.listTables()) {
        const schema = await lib.getTableSchema(table.table);
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
      for (const table of await lib.listTables()) {
        builder.addSheet(table.table, await lib.knex(table.table));
      }
    }

    if (argv.query) {
      builder.addSheet("Sheet1", await lib.knex.raw(argv.query));
    }

    builder.writeFile(filePath);

    console.log(filePath);

    await lib.destroy();
  }

  async createDump(argv) {
    const lib = await this.initLib(argv.conn, argv);
    const dumpFile = await lib.createDump(argv.name || null);
    console.log(dumpFile);
    await lib.destroy();
  }

  async loadDump(argv) {
    const lib = await this.initLib(argv.conn, argv);
    await lib.loadDump(argv.dump);
    await lib.destroy();
  }

  async openGui(argv) {
    const toTablePlusConnUri = (connUri) => {
      // Convert the conn uri protocol to one understood by TablePlus
      const tablePlusProtos = {
        mssql: "sqlserver",
        pg: "postgres",
        mysql2: "mysql",
      };
      const [protocol, ...rest] = connUri.split("://");

      // Sqlite is opened directly by opening the file with the default
      // application for its file extension, without setting a protocol.
      if (protocol === "sqlite3") {
        return rest[0];
      }

      // Rest of clients: build a connection uri with the protocol name
      // understood by TablePlus.
      return [tablePlusProtos[protocol] || protocol, ...rest].join("://");
    };

    const connUri = toTablePlusConnUri(this.stringifyConn(argv.conn, argv));

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
    const aliases = this.conf.get("aliases") || {};
    console.log(
      table(
        _.map(aliases, (conn, alias) => ({ alias, conn })),
        { headers: ["alias", "conn"] }
      )
    );
  }

  async addAlias(argv) {
    if (this.conf.get(`aliases.${argv.alias}`)) {
      this.error(`Alias '${argv.alias}' already exists`);
    }
    this.conf.set(`aliases.${argv.alias}`, argv.conn);
  }

  async removeAlias(argv) {
    if (!this.conf.get(`aliases.${argv.alias}`)) {
      this.error(`Alias '${argv.alias}' not found`);
    }
    this.conf.delete(`aliases.${argv.alias}`);
  }

  async initLib(conn) {
    const { conf, sshConf } =
      typeof conn === "string" ? this.resolveConn(conn) : conn;
    return await new Lib({ conf, sshConf }).connect();
  }

  resolveConn(connStr, argv = {}) {
    return resolveKnexConn(connStr, {
      client: argv.client,
      aliases: this.conf.get("aliases"),
    });
  }

  stringifyConn(connStr, argv = {}) {
    return stringifyKnexConn(connStr, {
      client: argv.client,
      aliases: this.conf.get("aliases"),
    });
  }

  error(message) {
    this.cli.showHelp();
    console.error(`\nError: ${message}\n`);
    process.exit(1);
  }
}

new CliApp();
