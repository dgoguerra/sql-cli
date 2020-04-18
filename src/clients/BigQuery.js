const Client = require("knex/lib/client");
const { makeEscape } = require("knex/lib/query/string");
const debug = require("debug")("knex:bigquery");

class Client_BigQuery extends Client {
  constructor(config) {
    super(config);
    this.useBigQueryTypes = config.useBigQueryTypes || false;
  }

  _driver() {
    const { BigQuery } = require("@google-cloud/bigquery");
    this.bigQueryTypes = [
      BigQuery.date,
      BigQuery.datetime,
      BigQuery.time,
      BigQuery.timestamp,
    ];

    return new BigQuery(this.connectionSettings);
  }

  schemaCompiler() {
    throw new Error("schema management not supported by BigQuery");
  }

  transaction() {
    throw new Error("transaction not supported by BigQuery");
  }

  wrapIdentifierImpl(value) {
    return value !== "*" ? `\`${value.replace(/`/g, "\\`")}\`` : "*";
  }

  // wrap the driver with a connection to keep track of the currently running job on the connection
  acquireRawConnection() {
    return Promise.resolve({ driver: this.driver, job: null });
  }

  // destroy by cancelling running job, if exists
  destroyRawConnection(connection) {
    if (connection.job != null) {
      return this.cancelQuery(connection);
    } else {
      return Promise.resolve();
    }
  }

  // connection should always be valid
  validateConnection(connection) {
    return Promise.resolve(true);
  }

  // execute the query with pagination
  _stream(connection, obj, stream, options) {
    const initPageQuery = { maxResults: 10000, ...options, autoPaginate: true };
    const query = { query: obj.sql, params: obj.bindings, ...obj.options };

    return new Promise((resolver, rejecter) => {
      stream.on("error", (err) => {
        this.cancelQuery(connection);
        rejecter(err);
      });
      stream.on("end", (results) => {
        connection.job = null;
        resolver(results);
      });

      const streamPages = (job, pageQuery) => {
        return job.getQueryResults(pageQuery).then((results) => {
          const rows = results[0];
          const nextQuery = results[1];

          this.processResponse({ response: rows }).forEach((row) =>
            stream.write(row)
          );

          if (nextQuery != null) {
            return streamPages(job, nextQuery);
          } else {
            return;
          }
        });
      };

      this._executeQuery(connection, query)
        .then((job) => streamPages(job, initPageQuery))
        .catch((err) => stream.emit("error", err))
        .then(() => stream.end());
    });
  }

  // execute the query with no pagination
  _query(connection, obj) {
    if (!obj || typeof obj === "string") {
      obj = { sql: obj };
    }

    const query = { query: obj.sql, params: obj.bindings, ...obj.options };

    return this._executeQuery(connection, query)
      .then((job) => job.getQueryResults({ autoPaginate: false }))
      .then(
        (results) => {
          connection.job = null;
          obj.response = results[0];
          return obj;
        },
        (err) => {
          this.cancelQuery(connection);
          throw err;
        }
      );
  }

  // execute a query and return the job if successful
  _executeQuery(connection, query) {
    Object.assign(query, { useLegacySql: false });

    const out = connection.driver
      .createQueryJob({
        ...query,
        defaultDataset: { datasetId: this.connectionSettings.database },
      })
      .then((results) => {
        connection.job = results[0];
        debug(`Job ${connection.job.id} started`);
        return connection.job.promise();
      })
      .then(() => connection.job.getMetadata())
      .then((metadata) => {
        const errors = metadata[0].status.errors;
        if (errors != null && errors.length > 0) {
          debug(`Job ${connection.job.id} failed with errors`);
          const error = new Error("Error executing query in BigQuery");
          error.bigQueryErrors = errors;
          throw error;
        }

        debug(`Job ${connection.job.id} completed`);
        return connection.job;
      });

    return Promise.resolve(out);
  }

  processResponse(obj, runner) {
    const rows = obj.response;
    if (!this.useBigQueryTypes) {
      rows.forEach((row) => {
        Object.keys(row).forEach((key) => {
          const value = row[key];
          if (value != null && this.bigQueryTypes.includes(value.constructor)) {
            row[key] = value.value;
          }
        });
      });
    }
    if (obj.output) {
      return obj.output.call(runner, rows);
    }
    return rows;
  }

  // cancel running job, if exists
  cancelQuery(connectionToKill) {
    if (connectionToKill.job == null) {
      return Promise.resolve();
    }

    const out = connectionToKill.job.cancel();
    connectionToKill.job = null;
    return Promise.resolve(out);
  }
}

Object.assign(Client_BigQuery.prototype, {
  dialect: "bigquery",
  driverName: "@google-cloud/bigquery",
  canCancelQuery: true,
  _escapeBinding: makeEscape(),
});

module.exports = Client_BigQuery;
