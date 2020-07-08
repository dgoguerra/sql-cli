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

  let { client: proto, connection: conn } = parseConn(connUri);

  // No client set manually, try to infer it from the conn URI's protocol
  if (!client) {
    const found = _.findKey(clientAliases, (val) => val.includes(proto));
    client = found || proto;
  }

  if (!client) {
    throw new Error(`Unknown Knex client, set one manually with --client`);
  }

  if (client === "bigquery") {
    client = require("./clients/BigQuery");
    conn.projectId = conn.host;
  }

  // Add default MySQL settings
  if (client === "mysql2" && typeof conn === "object") {
    conn = { charset: "utf8mb4", timezone: "+00:00", ...conn };
  }

  // Add default MSSQL settings
  if (client === "mssql" && typeof conn === "object") {
    conn = {
      options: { enableArithAbort: true },
      ...conn,
      port: Number(conn.port),
    };
  }

  return [{ client, connection: conn }, tableName];
}

function stringifyKnexConn(connStr, opts) {
  const [parsed] = resolveKnexConn(connStr, opts);
  const { client, connection: conn } = parsed;

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
