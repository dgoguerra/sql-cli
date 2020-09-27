const cp = require("child_process");
const CliApp = require("../CliApp");
const { stringifyConn } = require("../resolveKnexConn");

module.exports = {
  command: "open <conn>",
  description: "Open in configured GUI (such as TablePlus)",
  handler: async (argv) => {
    const connUri = await toTablePlusConnUri(argv.conn);

    // Remove password from output
    console.log(`Opening ${connUri.replace(/:([^\/]+?)@/, "@")} ...`);

    // Open conn uri with default application, should be
    // TablePlus if installed.
    await new Promise((resolve, reject) =>
      cp.exec(`open ${connUri}`, (err) => (err ? reject(err) : resolve()))
    );
  },
};

const toTablePlusConnUri = async (alias) => {
  // Convert the conn uri protocol to one understood by TablePlus
  const tablePlusProtos = {
    mssql: "sqlserver",
    pg: "postgres",
    mysql2: "mysql",
  };

  const { sshConf, conf } = CliApp.resolveConn(alias);
  const { client, connection: conn } = conf;

  // Sqlite is opened directly by opening the file with the default
  // application for its file extension, without setting a protocol.
  if (client === "sqlite3") {
    return rest[0];
  }

  // Might need to resolve the password from the system's keychain
  if (!conn.password && CliApp.aliasKeychains[alias]) {
    await CliApp.resolveConnPassword(alias, conn);
  }

  // Rest of clients: build a connection uri with the protocol name
  // understood by TablePlus.
  let connUri = stringifyConn({
    // Convert the conn uri protocol to one understood by TablePlus
    protocol: tablePlusProtos[client] || client,
    path: conn.filename, // only set in SQLite
    host: conn.host || conn.server,
    ...conn,
    sshHost: sshConf && sshConf.host,
    sshPort: sshConf && sshConf.port,
    sshUser: sshConf && sshConf.user,
    sshPassword: sshConf && sshConf.password,
  });

  // If the connection has SSH configured but no SSH password,
  // then TablePlus needs to auth with the user's private key.
  if (sshConf && sshConf.host && !sshConf.password) {
    connUri += "?usePrivateKey=true";
  }

  return connUri;
};
