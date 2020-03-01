#!/usr/bin/env node

const _ = require("lodash");
const chalk = require("chalk");
const yargs = require("yargs");
const prettyBytes = require("pretty-bytes");
const table = require("./src/table");
const Lib = require("./src/Lib");
const SqlRepl = require("./src/SqlRepl");

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
      .version()
      .demandCommand();

    cli.command({
      command: "list",
      aliases: ["ls"],
      desc: "List tables",
      handler: argv => this.listTables(argv)
    });

    cli.command({
      command: "show <table>",
      desc: "Show table structure",
      handler: argv => this.showTable(argv)
    });

    cli.command({
      command: "diff <table1> <table2>",
      desc: "Diff two tables",
      handler: argv => this.diffTables(argv)
    });

    cli.command({
      command: "*",
      alias: ["shell"],
      desc: "Run REPL shell",
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

    const schemaBefore = await lib.getSchema(argv.table1);
    const schemaAfter = await lib.getSchema(argv.table2);

    const colHash = c => `${c.type}:${c.maxLength}:${c.nullable}`;
    const colDescr = c => {
      let str = c.type;
      if (c.maxLength) {
        str += `(${c.maxLength})`;
      }
      if (c.nullable) {
        str += " nullable";
      }
      return str;
    };

    const columns = Object.keys(schemaBefore).map(key => {
      const colBefore = schemaBefore[key];
      const colAfter = schemaAfter[key];

      if (!colAfter) {
        return {
          column: key,
          status: "deleted",
          changes: chalk.red("deleted")
        };
      }

      if (colHash(colBefore) === colHash(colAfter)) {
        return { column: key, status: "similar" };
      }

      const before = chalk.red(colDescr(colBefore));
      const after = chalk.green(colDescr(colAfter));
      return {
        column: key,
        status: "changed",
        changes: `${before} -> ${after}`
      };
    });

    Object.keys(schemaAfter).forEach(key => {
      if (!schemaBefore[key]) {
        columns.push({
          column: key,
          status: "created",
          changes: chalk.green("created")
        });
      }
    });

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
