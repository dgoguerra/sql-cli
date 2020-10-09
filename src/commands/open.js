const cp = require("child_process");
const CliApp = require("../CliApp");
const { resolveProtocol, stringifyConn } = require("../connUtils");

module.exports = {
  command: "open <conn>",
  description: "Open in configured GUI (such as TablePlus)",
  builder: (yargs) =>
    yargs.option("dry-run", {
      description: "Only print the command to open the connection",
      type: "boolean",
    }),
  handler: async (argv) => {
    const connUri = await toTablePlusConnUri(argv.conn);

    // Remove password from output
    console.log(`Opening ${connUri.replace(/:([^\/]+?)@/, "@")} ...`);

    if (argv.dryRun) {
      console.log(`open ${connUri}`);
      return;
    }

    // Open conn uri with default application, should be
    // TablePlus if installed.
    await new Promise((resolve, reject) =>
      cp.exec(`open ${connUri}`, (err) => (err ? reject(err) : resolve()))
    );
  },
};

const toTablePlusConnUri = async (connUri) => {
  // Convert the conn uri protocol to one understood by TablePlus
  const tablePlusProtos = {
    mssql: "sqlserver",
    pg: "postgres",
    mysql2: "mysql",
  };

  const conn = CliApp.resolveConn(connUri);
  const protocol = resolveProtocol(conn.protocol);

  // Sqlite is opened directly by opening the file with the default
  // application for its file extension, without setting a protocol.
  if (protocol === "sqlite3") {
    return conn.filename;
  }

  // Might need to resolve the password from the system's keychain
  if (conn._alias && !conn.password && CliApp.aliasKeychains[conn._alias]) {
    await CliApp.lookupConnPassword(conn._alias, conn);
  }

  // Rest of clients: build a connection uri with the protocol name
  // understood by TablePlus.
  let tablePlusUri = stringifyConn({
    ...conn,
    // Convert the conn uri protocol to one understood by TablePlus
    protocol: tablePlusProtos[protocol] || protocol,
    host: conn.host || conn.server,
    sshHost: conn.sshHost,
    sshPort: conn.sshPort,
    sshUser: conn.sshUser,
    sshPassword: conn.sshPassword,
  });

  // If the connection has SSH configured but no SSH password,
  // then TablePlus needs to auth with the user's private key.
  if (conn.sshHost && !conn.sshPassword) {
    tablePlusUri += "?usePrivateKey=true";
  }

  return tablePlusUri;
};
