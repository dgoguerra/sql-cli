const BaseQueryBuilder = require("./BaseQueryBuilder");

// SQLite has a maximum of 999 SQL variables per prepared statement.
const MAX_SQLITE_VARS = 999;

class SqliteQueryBuilder extends BaseQueryBuilder {
  async streamInsert(stream) {
    await super.streamInsert(stream, {
      // By default SQLite has a rather small maximum of SQL variables per
      // prepared statement. Calculate maximum chunks size we should use
      // for the table we want to insert.
      chunkSize: await this._calculateChunkSize(),
    });
  }

  async _calculateChunkSize() {
    // Calculate the size of each chunk to bulk insert based on the
    // table's number of columns, to make sure if all columns were
    // inserted for all rows, we would still be below that hard limit.
    // This avoids the error: "SQLITE_ERROR: too many SQL variables"
    // on tables with a large amount of columns.
    const columns = await this.knex(this.builder._single.table).columnInfo();
    const chunkSize = MAX_SQLITE_VARS / Object.keys(columns).length;

    // Return chunk size casted to int
    return 0 | chunkSize;
  }
}

module.exports = SqliteQueryBuilder;
