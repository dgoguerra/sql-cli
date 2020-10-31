const Knex = require("knex");
const BaseQueryBuilder = require("./queries/BaseQueryBuilder");
const BaseSchemaBuilder = require("./schemas/BaseSchemaBuilder");
const {
  stringifyConn,
  resolveProtocol,
  getProtocolPort,
} = require("../connUtils");

const KNEX_DRIVERS = {
  sqlite: {
    clients: ["Client_SQLite3"],
    query: require("./queries/SqliteQueryBuilder"),
    schema: require("./schemas/SqliteSchemaBuilder"),
  },
  mysql: {
    clients: ["Client_MySQL", "Client_MySQL2"],
    schema: require("./schemas/MysqlSchemaBuilder"),
  },
  pg: {
    clients: ["Client_PG"],
    schema: require("./schemas/PgSchemaBuilder"),
  },
  mssql: {
    clients: ["Client_MSSQL"],
    query: require("./queries/MssqlQueryBuilder"),
    schema: require("./schemas/MssqlSchemaBuilder"),
  },
  bq: {
    clients: ["Client_BigQuery"],
    schema: require("./schemas/BigQuerySchemaBuilder"),
  },
  oracle: {
    clients: ["Client_Oracle", "Client_Oracledb"],
    schema: require("./schemas/OracleSchemaBuilder"),
  },
};

const createKnex = (conn) => {
  return hydrateKnex(buildKnex(conn));
};

const buildKnex = (conn) => {
  const rest = {
    log: {
      // Fix: avoid overly verbose warning during Knex migrations.
      // See https://github.com/knex/knex/issues/3921
      warn(msg) {
        msg.startsWith("FS-related option") || console.log(msg);
      },
    },
  };

  let client = resolveProtocol(conn.protocol) || conn.protocol;

  // Custom SQLite settings
  if (client === "sqlite3") {
    conn =
      conn.filename === ":memory:" ? ":memory:" : { filename: conn.filename };
    rest.useNullAsDefault = true;
  }

  // Custom BigQuery settings
  if (client === "bigquery") {
    client = require("./clients/BigQuery");
    conn.keyFilename = conn.params.keyFilename;
    conn.location = conn.params.location;
    conn.projectId = conn.host;
  }

  // Custom MySQL settings
  if (client === "mysql2") {
    conn.charset = conn.params.charset || "utf8mb4";
    conn.timezone = conn.params.timezone || "+00:00";
    if (!conn.port) {
      conn.port = getProtocolPort(client); // port is required in mysql2
    }
  }

  // Custom MSSQL settings
  if (client === "mssql") {
    conn.options = { enableArithAbort: true };
    conn.server = conn.host;
    delete conn.host;
  }

  // Fix: cleanup unused props from connection config to avoid error:
  // "Ignoring invalid configuration option passed to Connection".
  if (client === "mysql2") {
    const { host, port, user, password, database, charset, timezone } = conn;
    conn = { host, port, user, password, database, charset, timezone };
  }

  if (client === "pg") {
    const sslParamKeys = ["ca", "key", "cert"];
    const sslParams = { rejectUnauthorized: false };

    Object.keys(conn.params).forEach((key) => {
      if (sslParamKeys.includes(key)) {
        sslParams[key] = conn.params[key];
      }
    });

    // If any ssl config is set in the connection uri as params,
    // pass it to the pg connection config.
    if (Object.keys(sslParams).length > 1) {
      conn.ssl = sslParams;
    }
  }

  return Knex({ client, connection: conn, ...rest });
};

const hydrateKnex = (knex) => {
  // Methods to extend Knex object
  const KNEX_METHODS = {
    getDriver() {
      return getDriver(knex);
    },
    getUri() {
      return getUri(knex);
    },
  };

  // Proxy the created knex instance to use our custom methods
  const proxiedKnex = new Proxy(knex, {
    apply: (target, thisArg, argArray) => {
      const original = target.apply(thisArg, argArray);
      const extended = createQueryBuilder(proxiedKnex, original);

      // Extend Knex's QueryBuilder
      return new Proxy(original, {
        get: (target, prop) =>
          prop in extended ? extended[prop] : target[prop],
      });
    },
    get: (target, prop) => {
      if (prop in KNEX_METHODS) {
        return KNEX_METHODS[prop];
      }
      if (prop === "schema") {
        const original = target[prop];
        const extended = createSchemaBuilder(proxiedKnex, original);

        // Extend Knex's SchemaBuilder
        return new Proxy(original, {
          get: (target, prop) =>
            prop in extended ? extended[prop] : target[prop],
        });
      }
      return target[prop];
    },
  });

  return proxiedKnex;
};

const getUri = (knex) => {
  const { client, connection: conn } = knex.client.config;
  return stringifyConn({
    protocol: client,
    host: conn.server || conn.host,
    ...conn,
  });
};

const getDriver = (knex) => {
  const client = knex.client.constructor.name;
  for (const driver in KNEX_DRIVERS) {
    if (KNEX_DRIVERS[driver].clients.includes(client)) {
      return driver;
    }
  }
  return null;
};

const createQueryBuilder = (knex, builder) => {
  const { query } = KNEX_DRIVERS[getDriver(knex)] || {};
  if (query) {
    return new query(knex, builder);
  }
  return new BaseQueryBuilder(knex, builder);
};

const createSchemaBuilder = (knex, builder) => {
  const { schema } = KNEX_DRIVERS[getDriver(knex)] || {};
  if (schema) {
    return new schema(knex, builder);
  }
  return new BaseSchemaBuilder(knex, builder);
};

module.exports = { createKnex, buildKnex, hydrateKnex };
