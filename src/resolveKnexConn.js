const _ = require("lodash");
const parseConn = require("knex/lib/util/parse-connection");
const clientAliases = require("./clientAliases");

function resolveKnexConn(connStr, { client = null, aliases = {} } = {}) {
  let connUri;
  let tableName;

  const [uri, params] = connStr.split("?");
  const [protocol, rest] = uri.split("://");

  if (protocol && rest) {
    const [host, database, table] = rest.split("/");
    connUri = `${protocol}://${host}/${database}`;
    tableName = table;
  } else {
    const [host, database] = uri.split("/");
    connUri = aliases && aliases[host];
    tableName = database;
  }

  if (params) {
    connUri += connUri.indexOf("?") === -1 ? "?" : "&";
    connUri += params;
  }

  const conf = parseConn(connUri);

  if (client) {
    conf.client = client;
  } else {
    // No client set manually, try to infer it from the conn URI's protocol
    const found = _.findKey(clientAliases, (val) => val.includes(conf.client));
    if (found) {
      conf.client = found;
    }
  }

  if (!conf.client) {
    throw new Error(`Unknown Knex client, set one manually with --client`);
  }

  if (conf.client === "bigquery") {
    conf.client = require("./clients/BigQuery");
    conf.connection.projectId = conf.connection.host;
  }

  // Custom MySQL settings
  if (conf.client === "mysql2") {
    conf.connection = { charset: "utf8mb4", timezone: "+00:00", ...conf.connection };
  }

  // Custom MSSQL settings
  if (conf.client === "mssql") {
    conf.connection = {
      options: { enableArithAbort: true },
      ...conf.connection,
      port: Number(conf.connection.port),
    };
  }

  // Custom SQLite settings
  if (conf.client === "sqlite3") {
    conf.connection = { filename: rest };
    conf.useNullAsDefault = true;
  }

  return [conf, tableName];
}

function stringifyKnexConn(connStr, opts) {
  const [conf] = resolveKnexConn(connStr, opts);
  const { client, connection: conn } = conf;

  const auth =
    conn.user && conn.password
      ? `${encodeURIComponent(conn.user)}:${encodeURIComponent(conn.password)}@`
      : conn.user
      ? encodeURIComponent(conn.user)
      : "";

  const host = (conn.host || conn.server) + (conn.port ? `:${conn.port}` : "");

  return `${client}://${auth}${host}/${conn.database}`;
}

module.exports = { resolveKnexConn, stringifyKnexConn };
