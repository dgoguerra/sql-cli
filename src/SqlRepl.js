const repl = require("repl");
const _ = require("lodash");
const chalk = require("chalk");
const table = require("./table");

class SqlRepl {
  constructor(lib) {
    this.lib = lib;

    // Buffer of the current query being inputted, since it might be
    // a multiline quere, received line by line.
    this.query = "";

    // Queue to run queries and schedule resolving this.run() sequentially.
    // This allows waiting for the input queries execution when piping SQL
    // statements as stdin.
    this.queue = Promise.resolve();
  }

  async run() {
    this.server = repl.start({
      prompt: "→ ",
      eval: (...args) => this.evalLine(...args),
      writer: (...args) => this.formatResult(...args),
    });

    this.server.defineCommand("tables", {
      help: "List available tables",
      action: () => this.listSchemaTables(),
    });

    this.server.defineCommand("table", {
      help: "List available tables",
      action: (table) => this.listTableColumns(table),
    });

    return new Promise((resolve) => {
      this.server.on("close", () => {
        // Resolve promise when pending queries finish
        this.queue = this.queue.then(() => resolve());
      });
    });
  }

  async listSchemaTables() {
    const tables = await this.lib.listTables();
    const rows = _.sortBy(tables, (row) => -row.bytes).map((row) => ({
      ...row,
      bytes: row.prettyBytes,
    }));

    console.log(table(rows, { headers: ["table", "rows", "bytes"] }));

    this.server.displayPrompt();
  }

  async listTableColumns(tableName) {
    const columns = await this.lib.getTableSchema(tableName);
    const rows = Object.keys(columns).map((key) => {
      const column = columns[key];
      return {
        column: key,
        type: column.fullType,
        nullable: column.nullable,
      };
    });

    console.log(table(rows));

    this.server.displayPrompt();
  }

  async evalLine(line, context, file, next) {
    // Aggregate input lines until the query to run is finished
    // (when the line ends with ';').
    this.query += " " + line.trim();

    // There is no query in progress
    if (!this.query) {
      return next();
    }

    // Line is an unfinished query, print a different prompt
    // and do nothing else.
    if (!this.query.endsWith(";")) {
      process.stdout.write("... ");
      return;
    }

    // The whole query has been inputted, run it
    const query = this.query;
    this.query = "";
    this.queue = this.queue.then(() =>
      this.lib.knex
        .raw(query)
        .then((result) => next(null, result))
        .catch((err) => next(null, err))
    );
  }

  formatResult(result) {
    if (!result) {
      return;
    }

    if (result instanceof Error) {
      const err = result;
      return err.sqlMessage
        ? `Error ${err.code}: ${err.sqlMessage}`
        : `Error: ${err.message}`;
    }

    // Knex clients return different formats as result of knex.raw(query)
    const rows = Array.isArray(result)
      ? Array.isArray(result[0])
        ? result[0]
        : result
      : result.rows;

    if (!rows || !rows.length) {
      return chalk.grey("(no results)");
    }

    return (
      table(rows, {
        headers: Object.keys(rows[0]),
        format: (val) => (val === null ? chalk.grey("null") : val),
      }) + `\n${chalk.grey(`(${rows.length} rows)`)}`
    );
  }
}

module.exports = SqlRepl;
