#!/usr/bin/env node

const _ = require("lodash");
const chalk = require("chalk");
const yargs = require("yargs");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const Lib = require("./src/Lib");
const SqlRepl = require("./src/SqlRepl");
const { diffColumns } = require("./src/schemaDiff");

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
      .option("conn", {
        description: "Knex connection URI",
        type: "string"
      })
      .help()
      .alias("h", "help")
      .version()
      .demandCommand();

    cli.command({
      command: "list",
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
      command: "*",
      aliases: ["shell"],
      description: "Run REPL shell",
      handler: argv => this.runInteractiveShell(argv)
    });

    return cli;
  }

  async listTables(argv) {
    const lib = this.initLib(argv);
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
    const lib = this.initLib(argv);

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
    const lib = this.initLib(argv);

    if (!(await lib.tableExists(argv.table1))) {
      this.error(`Table '${argv.table1}' not found`);
    }
    if (!(await lib.tableExists(argv.table2))) {
      this.error(`Table '${argv.table2}' not found`);
    }

    const columns = diffColumns(
      await lib.getSchema(argv.table1),
      await lib.getSchema(argv.table2)
    )
      .map(col => {
        switch (col.status) {
          case "deleted":
            col.column = chalk.red(col.column);
            col.changes = chalk.red(col.descBefore);
            break;
          case "created":
            col.column = chalk.green(col.column);
            col.changes = chalk.green(col.descAfter);
            break;
          case "changed":
            col.changes = `${chalk.red(col.descBefore)} -> ${chalk.green(
              col.descAfter
            )}`;
            break;
          case "similar":
            col.changes = col.descBefore;
            break;
        }
        return col;
      })
      // If running with --quiet, hide rows without changes
      .filter(col => (argv.quiet ? col.status !== "similar" : true));

    console.log(
      table(
        columns.filter(c => c.changes),
        {
          headers: ["column", "changes"],
          // Disable default rows formatting, since some fields
          // already have diff colors applied.
          format: val => val
        }
      )
    );

    const facts = _(columns)
      .countBy("status")
      .map((num, status) => ({ num, text: `${num}x ${status}` }))
      .orderBy(c => -c.num)
      .map("text")
      .join(", ");

    if (facts) {
      console.log(facts);
      console.log("");
    }

    await lib.destroy();
  }

  async runInteractiveShell(argv) {
    const lib = this.initLib(argv);
    await new SqlRepl(lib).run();
    await lib.destroy();
  }

  initLib(argv) {
    return new Lib({
      knex: {
        client: argv.client || "mysql2",
        connection: argv.conn
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
