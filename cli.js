#!/usr/bin/env node

const _ = require("lodash");
const yargs = require("yargs");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const Lib = require("./src/Lib");
const SqlRepl = require("./src/SqlRepl");
const { diffColumns, diffSchemas } = require("./src/schemaDiff");

class CliApp {
  constructor() {
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
      command: "* [conn]",
      aliases: ["shell"],
      description: "Run REPL shell",
      handler: argv => this.runInteractiveShell(argv)
    });

    return cli;
  }

  async listTables(argv) {
    const lib = this.initLib(argv.conn, argv);
    const tables = await lib.listTables();

    const rows = await Promise.all(
      tables.map(async table => ({ table, ...(await lib.getTableInfo(table)) }))
    );

    const formatted = _.sortBy(rows, row => -row.bytes).map(row => {
      row.bytes = prettyBytes(row.bytes);
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
    const lib = this.initLib(argv.conn, argv);

    const schema = await lib.getSchema(argv.table);

    const formatted = _.map(schema, (val, key) => ({
      column: key,
      type: val.maxLength ? `${val.type}(${val.maxLength})` : val.type,
      nullable: val.nullable
    }));

    console.log(table(formatted));

    await lib.destroy();
  }

  async diffTables(argv) {
    const extractTable = str => {
      var regex = /:\/\/.+\/.+\/(.+)/g;
      var arr = regex.exec(str);
      return arr && arr.length > 1 ? arr[1] : null;
    };

    const parseConn = conn => {
      if (conn.indexOf("/") === -1) {
        return [null, conn];
      }
      const table = extractTable(conn);
      if (table) {
        conn = conn.replace(new RegExp(`\/${table}$`, "g"), "");
      }
      return [conn, table];
    };

    const [conn1, table1] = parseConn(argv.table1);
    const [conn2, table2] = parseConn(argv.table2);

    const lib1 = this.initLib(conn1 || argv.conn, argv);
    const lib2 = this.initLib(conn2 || argv.conn, argv);

    // Diffing two tables columns
    if (table1 && table2) {
      if (!(await lib1.tableExists(table1))) {
        this.error(`Table '${argv.table1}' not found in before schema`);
      }
      if (!(await lib2.tableExists(table2))) {
        this.error(`Table '${argv.table2}' not found in after schema`);
      }

      const { columns, summary } = diffColumns(
        await lib1.getSchema(argv.table1),
        await lib2.getSchema(argv.table2)
      ).filter(col => {
        // If running with --quiet, hide rows without changes
        return argv.quiet ? col.status !== "similar" : true;
      });

      console.log(
        table(
          columns.map(col => ({
            column: col.displayColumn,
            type: col.displayType
          })),
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
        const tableNames = await lib.listTables();
        const tablesArr = await Promise.all(
          tableNames.map(async table => ({
            table,
            ...(await lib.getTableInfo(table)),
            schema: await lib.getSchema(table)
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

  initLib(conn, argv = {}) {
    return new Lib({
      knex: {
        client: argv.client || "mysql2",
        connection: conn
      }
    });
  }

  error(message) {
    this.cli.showHelp();
    console.error(`\nError: ${message}\n`);
    process.exit(1);
  }
}

new CliApp();
