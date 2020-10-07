const _ = require("lodash");

// Known protocol aliases (based on aliases in https://github.com/xo/usql)
const PROTOCOL_ALIASES = {
  mysql2: ["my", "mysql", "maria", "aurora", "mariadb", "percona"],
  pg: ["pgsql", "postgres", "postgresql"],
  sqlite3: ["sq", "file", "sqlite"],
  mssql: ["ms", "sqlserver", "microsoftsqlserver"],
  bigquery: ["bq"],
};

function resolveProtocol(client) {
  return _.findKey(
    PROTOCOL_ALIASES,
    (val, key) => key === client || val.includes(client)
  );
}

function parseUriParams(params) {
  return (params || "").split("&").reduce((acc, param) => {
    if (param) {
      const [key, val] = param.split("=");
      acc[key] = val || true;
    }
    return acc;
  }, {});
}

function parseUri(conn) {
  const uriRegex = /^([^@]+@)?([^\/]*)(.+)?$/;
  const parsed = { _uri: conn };

  const [connUri, params] = conn.split("?");
  let [protocol, uriRest] = connUri.split("://");

  parsed.params = parseUriParams(params);

  if (protocol && uriRest) {
    parsed.protocol = protocol;
  } else {
    uriRest = protocol;
  }

  const [matched, auth, domain, path] = uriRest.match(uriRegex);

  if (auth) {
    const [user, password] = auth.replace(/@$/, "").split(":");
    if (user) {
      parsed.user = decodeURIComponent(user);
    }
    if (password) {
      parsed.password = decodeURIComponent(password);
    }
  }

  if (path) {
    parsed.path = path;
  }

  const [host, port] = domain.split(":");

  parsed.host = host;
  if (port) {
    parsed.port = Number(port);
  }

  return parsed;
}

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

function resolveConn(connUri, { aliases = {} } = {}) {
  let conn = parseUri(connUri);

  if (!conn.protocol) {
    const previous = conn;
    conn = parseUri(resolveConnAlias(previous.host, { aliases }));
    conn._alias = previous.host;
    if (previous.path) {
      conn.path += previous.path;
    }
  }

  if (!conn.protocol) {
    throw new Error(
      `Unknown connection URI, cannot parse or resolve it from an alias`
    );
  }

  const protocols = conn.protocol.split("+");
  if (protocols.includes("ssh")) {
    const pathConn = parseUri(conn.path.replace(/^\//, ""));

    conn.protocol = protocols.filter((p) => p !== "ssh").join("+");
    conn.sshHost = conn.host;
    conn.sshPort = conn.port;
    conn.sshUser = conn.user;
    conn.sshPassword = conn.password;

    conn.host = pathConn.host;
    conn.port = pathConn.port;
    conn.user = pathConn.user;
    conn.password = pathConn.password;
    conn.path = pathConn.path;
  }

  // Custom SQLite settings
  if (resolveProtocol(conn.protocol) === "sqlite3") {
    // the host + path forms a path to a local file
    conn.filename = `${conn.host || ""}${conn.path || ""}`;

    // Extract optional table name from its filename path
    const matches = conn.filename.match(/^(.*[^\/]+\.[^\/]+)\/([^\/\.]+)?$/);
    if (matches) {
      conn.filename = matches[1];
      conn._table = matches[2];
    }

    return conn;
  }

  const [, database, table] = (conn.path || "").split("/");
  conn.database = database;
  conn._table = table;

  return conn;
}

function stringifyConn({
  protocol,
  host,
  path = null,
  port = null,
  user,
  password = null,
  database,
  sshHost = null,
  sshPort = 22,
  sshUser = process.env.USER,
  sshPassword,
}) {
  const formatHost = (host, port) => (port ? `${host}:${port}` : host);

  const formatAuth = (user, password) =>
    user && password
      ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@`
      : user
      ? `${encodeURIComponent(user)}@`
      : "";

  // Only used for SQLite
  if (path) {
    return `${protocol}://${path}`;
  }

  let connUri = formatAuth(user, password) + formatHost(host, port);

  if (database) {
    connUri += `/${database}`;
  }

  if (sshHost) {
    const sshConnUri =
      formatAuth(sshUser, sshPassword) + formatHost(sshHost, sshPort);
    return `${protocol}+ssh://${sshConnUri}/${connUri}`;
  }

  return `${protocol}://${connUri}`;
}

module.exports = { resolveProtocol, parseUri, resolveConn, stringifyConn };
