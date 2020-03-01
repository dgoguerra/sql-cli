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
    return new Promise(resolve => {
      repl
        .start({
          prompt: "=> ",
          eval: (...args) => this.evalQuery(...args),
          writer: (...args) => this.formatResult(...args)
        })
        .on("close", () => {
          // Resolve promise when pending queries finish
          this.queue = this.queue.then(() => resolve());
        });
    });
  }

  async evalQuery(query, context, file, next) {
    query = query.trim();
    if (!query) {
      return next();
    }

    this.queue = this.queue.then(async () => {
      try {
        const [rows] = await this.lib.knex.raw(query);
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
    if (!result.length) {
      return "(no results)";
    }
    return table(result, Object.keys(result[0]));
  }
}

module.exports = SqlRepl;
