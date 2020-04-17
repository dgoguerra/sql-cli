#!/usr/bin/env node

const _ = require("lodash");
const Conf = require("conf");
const yargs = require("yargs");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const Lib = require("./src/Lib");
const SqlRepl = require("./src/SqlRepl");
const { resolveKnexConn } = require("./src/resolveKnexConn");
const { diffColumns, diffSchemas } = require("./src/schemaDiff");

class CliApp {
  constructor() {
    this.conf = new Conf({
      defaults: { aliases: {} }
    });
    this.cli = this.buildYargs();
    this.argv = this.cli.argv;
  }

  buildYargs() {
    const cli = yargs
      .option("client", {
        alias: "c",
        description: "Knex client adapter",
        type: "string"
      })
      .help()
      .alias("h", "help")
      .version()
      .demandCommand();

    cli.command({
      command: "list <conn>",
      aliases: ["ls"],
      description: "List tables",
      handler: argv => this.listTables(argv)
    });

    cli.command({
      command: "show <table>",
      description: "Show table structure",
      handler: argv => this.showTable(argv)
    });

    cli.command({
      command: "diff <table1> <table2>",
      description: "Diff two tables",
      builder: yargs =>
        yargs.option("quiet", {
          alias: "q",
          description: "Quiet output, hiding rows without changes",
          type: "boolean"
        }),
      handler: argv => this.diffTables(argv)
    });

    cli.command({
      command: "alias <action>",
      description: "Manage saved connection aliases",
      builder: yargs =>
        yargs
          .command({
            command: "list",
            aliases: ["ls"],
            description: "List existing aliases",
            handler: () => this.listAliases()
          })
          .command({
            command: "add <alias> <conn>",
            description: "Add new alias",
            handler: argv => this.addAlias(argv)
          })
          .command({
            command: "remove <alias>",
            aliases: ["rm"],
            description: "Remove saved alias",
            handler: argv => this.removeAlias(argv)
          })
          .demandCommand()
    });

    cli.command({
      command: "shell <conn>",
      aliases: ["sh"],
      description: "Run REPL shell",
      handler: argv => {
        this.runInteractiveShell(argv);
      }
    });

    return cli;
  }

  async listTables(argv) {
    const lib = this.initLib(argv.conn, argv);
    const tables = await lib.listTables();

    const formatted = _.sortBy(tables, row => -row.bytes).map(row => {
      row.rows = row.rows;
      row.bytes = row.bytes === undefined ? "" : prettyBytes(row.bytes);
      return row;
    });

    console.log(
      table(formatted, {
        headers: ["table", "rows", "bytes"]
      })
    );

    await lib.destroy();
  }

  async showTable(argv) {
    const [conn, tableName] = this.resolveConn(argv.table, argv);

    if (!tableName) {
      this.error("No table was specified in the connection");
    }

    const lib = this.initLib(conn);
    const schema = await lib.getSchema(tableName);

    const formatted = _.map(schema, (val, key) => ({
      column: key,
      type: val.maxLength ? `${val.type}(${val.maxLength})` : val.type,
      nullable: val.nullable
    }));

    console.log(table(formatted));

    await lib.destroy();
  }

  async diffTables(argv) {
    const [conn1, table1] = this.resolveConn(argv.table1, argv);
    const [conn2, table2] = this.resolveConn(argv.table2, argv);

    const lib1 = this.initLib(conn1);
    const lib2 = this.initLib(conn2);

    // Diffing two tables columns
    if (table1 && table2) {
      if (!(await lib1.tableExists(table1))) {
        this.error(`Table '${argv.table1}' not found in 'before' schema`);
      }
      if (!(await lib2.tableExists(table2))) {
        this.error(`Table '${argv.table2}' not found in 'after' schema`);
      }

      const { columns, summary } = diffColumns(
        await lib1.getSchema(table1),
        await lib2.getSchema(table2)
      );

      const formatted = columns
        .filter(col => {
          // If running with --quiet, hide rows without changes
          return argv.quiet ? col.status !== "similar" : true;
        })
        .map(col => ({
          column: col.displayColumn,
          type: col.displayType
        }));

      console.log(
        table(
          formatted,
          // Disable default rows formatting, since the fields
          // already have diff colors applied.
          { headers: ["column", "type"], format: val => val }
        )
      );

      if (summary) {
        console.log(summary);
        console.log("");
      }
    }

    // Diffing all tables of two schemas
    else {
      const getTablesInfo = async lib => {
        const tablesArr = await Promise.all(
          (await lib.listTables()).map(async row => ({
            ...row,
            schema: await lib.getSchema(row.table)
          }))
        );
        return _.keyBy(tablesArr, "table");
      };

      const tables = diffSchemas(
        await getTablesInfo(lib1),
        await getTablesInfo(lib2)
      ).map(table => ({
        table: table.displayTable,
        rows: table.displayRows,
        bytes: table.displayBytes,
        columns: table.displaySummary
      }));

      console.log(
        table(tables, {
          headers: ["table", "rows", "bytes", "columns"],
          // Disable default rows formatting, since the fields
          // already have diff colors applied.
          format: val => val
        })
      );
    }

    await lib1.destroy();
    await lib2.destroy();
  }

  async runInteractiveShell(argv) {
    const lib = this.initLib(argv.conn, argv);

    // Check db connection before dropping the user to the shell,
    // to avoid waiting until a query is run to know that the
    // connection is invalid.
    await lib.checkConnection();

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

  initLib(conn) {
    if (typeof conn === "string") {
      const [parsed] = this.resolveConn(conn);
      conn = parsed;
    }
    return new Lib({
      knex: conn,
      proxy: conn.connection.proxy
    });
  }

  resolveConn(connStr, argv = {}) {
    return resolveKnexConn(connStr, {
      client: argv.client,
      aliases: this.conf.get("aliases")
    });
  }

  error(message) {
    this.cli.showHelp();
    console.error(`\nError: ${message}\n`);
    process.exit(1);
  }
}

new CliApp();
