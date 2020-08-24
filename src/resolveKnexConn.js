const parseConn = require("knex/lib/util/parse-connection");
const { getClient } = require("./clientAliases");

function resolveConnAlias(connAlias, { aliases = {} } = {}) {
  const [uri, params] = connAlias.split("?");
  const [alias, table] = uri.split("/");
  let connUri = aliases && aliases[alias];
  if (table) {
    connUri += `/${table}`;
  }
  if (params) {
    connUri += connUri.indexOf("?") === -1 ? "?" : "&";
    connUri += params;
  }
  return connUri;
}

function resolveKnexConn(connUri, { client = null, aliases = {} } = {}) {
  if (!connUri.includes("://")) {
    connUri = resolveConnAlias(connUri, { aliases });
  }

  if (!connUri.includes("://")) {
    throw new Error(
      `Unknown connection URI, cannot parse or resolve it to an alias`
    );
  }

  const [uri, params] = connUri.split("?");
  let [protocol, uriPath] = uri.split("://");

  let sshConf = null;
  const protocols = protocol.split("+");
  if (protocols.includes("ssh")) {
    protocol = protocols.filter((p) => p !== "ssh").join("+");
    const { database, ...sshParams } = parseConn(`ssh://${uriPath}`).connection;
    uriPath = database;
    sshConf = sshParams;
  }

  let table;

  // SQLite case, the whole uriPath is a filename
  if (uriPath.startsWith("/")) {
    const parts = uriPath.split("/");
    const dbTable = parts.pop();
    const dbName = parts.pop();

    // Detect optional table name in the uri
    if (dbTable && dbName && !dbTable.includes(".") && dbName.includes(".")) {
      uriPath = `${parts.join("/")}/${dbName}`;
      connUri = `${protocol}://${uriPath}`;
      table = dbTable;
    } else {
      connUri = `${protocol}://${uriPath}`;
    }
  } else {
    const [host, dbName, dbTable] = uriPath.split("/");
    connUri = `${protocol}://${host}/${dbName}`;
    table = dbTable;
  }

  if (params) {
    connUri += (connUri.indexOf("?") === -1 ? "?" : "&") + params;
  }

  const conf = parseConn(connUri);

  // The uri protocol is the client, or an alias of the client.
  // This may be overriden through the option --client.
  conf.client = client || getClient(conf.client) || conf.client;

  if (!conf.client) {
    throw new Error(`Unknown Knex client, set one manually with --client`);
  }

  if (conf.client === "bigquery") {
    conf.client = require("./clients/BigQuery");
    conf.connection.projectId = conf.connection.host;
  }

  // Custom MySQL settings
  if (conf.client === "mysql2") {
    conf.connection = {
      charset: "utf8mb4",
      timezone: "+00:00",
      ...conf.connection,
    };
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
    conf.connection = { filename: uriPath };
    conf.useNullAsDefault = true;
  }

  return { sshConf, conf, table };
}

function stringifyKnexConn(connUri, opts) {
  const { conf } = resolveKnexConn(connUri, opts);
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
