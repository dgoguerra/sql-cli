const repl = require("repl");
const _ = require("lodash");
const chalk = require("chalk");
const table = require("./table");
const { Readable } = require("stream");

class SqlRepl {
  constructor(lib, { input = process.stdin } = {}) {
    this.lib = lib;
    this.input = this.inputToStream(input);

    // Wether the REPL is being run interactively. If stdin is a script
    // piped into the REPL, we don't want to print the prompt or number
    // of rows after the results of a query.
    this.tty = input.isTTY;

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
      prompt: this.tty ? "→ " : "",
      input: this.input,
      output: process.stdout,
      terminal: false, // do not print non-tty input to stdout
      eval: (...args) => this.evalLine(...args),
      writer: (...args) => this.formatResult(...args),
    });

    this.server.defineCommand("tables", {
      help: "List available tables",
      action: () => this.runAction(() => this.listSchemaTables()),
    });

    this.server.defineCommand("table", {
      help: "List available tables",
      action: (table) => this.runAction(() => this.listTableColumns(table)),
    });

    return new Promise((resolve) => {
      this.server.on("close", () => {
        // Resolve promise when pending queries finish
        this.queue = this.queue.then(() => resolve());
      });
    });
  }

  async runAction(actionFunc) {
    try {
      const result = await actionFunc();
      console.log(this.formatResult(result));
    } catch (err) {
      console.log(this.formatError(err));
    }
    this.server.displayPrompt();
  }

  async listSchemaTables() {
    const tables = await this.lib.schema.listTables();
    return _.sortBy(tables, (row) => -row.bytes).map((row) => ({
      table: row.table,
      rows: row.rows,
      bytes: row.prettyBytes,
    }));
  }

  async listTableColumns(tableName) {
    if (!tableName) {
      throw new Error(`Missing 'table' argument`);
    }

    const columns = await this.lib(tableName).columnInfo();
    if (!Object.keys(columns).length) {
      throw new Error(`Table '${tableName}' not found`);
    }

    return Object.keys(columns).map((key) => ({
      column: key,
      type: columns[key].fullType,
      nullable: columns[key].nullable,
    }));
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
      return this.formatError(result);
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
      table(rows, { headers: Object.keys(rows[0]) }) +
      (this.tty ? `\n\n${chalk.grey(`(${rows.length} rows)`)}` : "")
    );
  }

  formatError(err) {
    return err.sqlMessage
      ? `Error ${err.code}: ${err.sqlMessage}`
      : `Error: ${err.message}`;
  }

  inputToStream(input) {
    // Already a readable stream
    if (input.readable) {
      return input;
    }

    // Input should be a string or an array of strings.
    // Build a readable stream to expose that input.
    return new Readable({
      read() {
        const arr = Array.isArray(input) ? input : [input];
        arr.forEach((val) => this.push(val));
        this.push(null);
      },
    });
  }
}

module.exports = SqlRepl;
