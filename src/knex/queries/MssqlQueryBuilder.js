const writer = require("flush-write-stream");
const BaseQueryBuilder = require("./BaseQueryBuilder");
const { chunk, runPipeline } = require("../../streamUtils");

class MssqlQueryBuilder extends BaseQueryBuilder {
  async streamInsert(stream) {
    const { table } = this.builder._single;
    const primaryKey = await this.knex.schema.getPrimaryKey(table);
    const columns = await this.knex(table).columnInfo();

    await runPipeline(
      stream,
      chunk(),
      writer.obj((rows, enc, next) =>
        this._bulkMssqlInsert(rows, { table, columns, primaryKey })
          .then(() => next())
          .catch((err) => next(err))
      )
    );
  }

  async _bulkMssqlInsert(
    rows,
    { table, columns = {}, primaryKey = null } = {}
  ) {
    // Get underlying MSSQL client. See: https://www.npmjs.com/package/mssql
    const mssql = this.knex.client.driver;
    const columnKeys = Object.keys(columns);

    const customTypes = {
      DECIMAL: () => mssql.DECIMAL(32, 16),
      TEXT: () => mssql.NVARCHAR(mssql.MAX),
    };

    const msTable = new mssql.Table(table);
    msTable.create = false;

    columnKeys.forEach((colKey) => {
      const { type, nullable } = columns[colKey];
      const msTypeName = type.toUpperCase();

      const msType = customTypes[msTypeName] || mssql[msTypeName];
      if (!msType) {
        throw new Error(
          `Unknown MSSQL column type '${msTypeName}'. A valid ` +
            `data type should be used. See: ` +
            `https://www.npmjs.com/package/mssql#data-types`
        );
      }

      msTable.columns.add(colKey, msType, {
        primary: primaryKey.includes(colKey),
        length: Infinity,
        nullable,
      });
    });

    rows.forEach((row) => {
      msTable.rows.add(...columnKeys.map((col) => row[col]));
    });

    const mssqlConn = await this.knex.client.acquireConnection();

    try {
      await new mssql.Request(mssqlConn).bulk(msTable, {
        keepNulls: true,
      });
    } catch (err) {
      await this.knex.client.releaseConnection(mssqlConn);
      throw err;
    }

    await this.knex.client.releaseConnection(mssqlConn);
  }
}

module.exports = MssqlQueryBuilder;
