const repl = require("repl");
const table = require("./table");

class SqlRepl {
  constructor(lib) {
    this.lib = lib;

    // Queue to run queries and schedule resolving this.run() sequentially.
    // This allows waiting the the input queries execution when piping SQL
    // statements as stdin.
    this.queue = Promise.resolve();
  }

  async run() {
    this.server = repl.start({
      prompt: "→ ",
      eval: (...args) => this.evalQuery(...args),
      writer: (...args) => this.formatResult(...args),
    });

    this.server.defineCommand("tables", {
      help: "List available tables",
      action: () => this.listSchemaTables(),
    });

    return new Promise((resolve) => {
      this.server.on("close", () => {
        // Resolve promise when pending queries finish
        this.queue = this.queue.then(() => resolve());
      });
    });
  }

  async listSchemaTables() {
    const schema = await this.lib.getDatabaseSchema();
    const rows = [];

    Object.keys(schema).forEach((table) => {
      rows.push({
        table,
        rows: schema[table].rows,
        columns: Object.keys(schema[table].schema).join(","),
      });
    });

    console.log(table(rows, { headers: ["table", "rows", "columns"] }));

    this.server.displayPrompt();
  }

  async evalQuery(query, context, file, next) {
    query = query.trim();
    if (!query) {
      return next();
    }

    this.queue = this.queue.then(async () => {
      try {
        const rows = await this.lib.knex.raw(query);
        next(null, rows);
      } catch (err) {
        next(null, err);
      }
    });
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
    const rows = result.length && result[0].length ? result[0] : result;
    if (!rows.length) {
      return "(no results)";
    }
    return table(rows, Object.keys(rows[0]));
  }
}

module.exports = SqlRepl;
