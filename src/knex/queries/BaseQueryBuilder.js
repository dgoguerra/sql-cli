const writer = require("flush-write-stream");
const { chunk, runPipeline } = require("../../streamUtils");

class BaseQueryBuilder {
  constructor(knex, builder) {
    this.knex = knex;
    this.builder = builder;
  }

  async hasRows() {
    const rows = await this.builder.select(this.knex.raw(1)).limit(1);
    return !!rows.length;
  }

  async countRows() {
    const [row] = await this.builder.count({ count: "*" });
    return Number(row.count);
  }

  async streamInsert(stream, { chunkSize = 500 } = {}) {
    await runPipeline(
      stream,
      chunk(chunkSize),
      writer.obj((rows, enc, next) =>
        this.knex(this.builder._single.table)
          .insert(rows)
          .then(() => next())
          .catch((err) => next(err))
      )
    );
  }
}

module.exports = BaseQueryBuilder;
