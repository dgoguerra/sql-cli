const fs = require("fs");
const net = require("net");
const path = require("path");
const debug = require("debug")("sql-cli:ssh");
const { Client } = require("ssh2");

const SSH_CONN_DEFAULTS = {
  host: "localhost",
  port: 22,
  user: process.env.USER,
  password: null,
  privateKey: null,
};

const SSH_DEFAULT_KEYS = ["~/.ssh/id_dsa", "~/.ssh/id_ecdsa", "~/.ssh/id_rsa"];

const sshClient = async (opts) => {
  const client = new Client();

  client.on("error", (err) => {
    debug(`ssh2: ${err.message}`);
  });

  opts = { ...SSH_CONN_DEFAULTS, ...opts };

  // A password was provided, try password authentication first
  if (
    opts.password &&
    (await attemptPasswordAuth(client, opts, opts.password))
  ) {
    return client;
  }

  // Public key auth. Try to connect using a private key supplied by the user
  // or in a default path, to mimic behaviour of the OpenSSH binary `ssh`.
  const identKeys = [...SSH_DEFAULT_KEYS];

  if (opts.privateKey) {
    if (typeof opts.privateKey === "string") {
      opts.privateKey = [opts.privateKey];
    }
    // The user-supplied keys are tried first
    opts.privateKey.forEach((key) => identKeys.unshift(key));
  }

  for (const keyFile of identKeys) {
    if (await attemptPublicKeyAuth(client, opts, keyFile)) {
      return client;
    }
  }

  throw new Error("All configured authentication methods failed");
};

const forwardPort = (client, { srcHost, srcPort, dstHost, dstPort }) => {
  return new Promise((resolve) => {
    const sockServer = net.createServer((sock) => {
      client.forwardOut(srcHost, srcPort, dstHost, dstPort, (err, stream) => {
        if (err) {
          sock.emit("error", err);
          return;
        }
        sock.pipe(stream).pipe(sock);
      });
    });

    client.on("close", () => {
      sockServer && sockServer.close();
    });

    sockServer.listen(srcPort, srcHost, () => {
      resolve(sockServer);
    });
  });
};

const attemptAuth = (client, opts) => {
  return new Promise((resolve) => {
    const onReady = () => {
      cleanListeners();
      resolve(true);
    };
    const onError = (err) => {
      debug(err.message);
      cleanListeners();
      resolve(false);
    };
    const cleanListeners = () => {
      client.removeListener("ready", onReady);
      client.removeListener("error", onError);
    };

    client.once("ready", onReady);
    client.once("error", onError);
    client.connect(opts);
  });
};

const attemptPasswordAuth = (client, opts, password) => {
  const { host, port, user } = opts;
  debug(`auth to ${user}@${host}:${port} with password ...`);
  return attemptAuth(client, { host, port, user, password });
};

const attemptPublicKeyAuth = (client, opts, keyFile) => {
  const { host, port, user } = opts;
  debug(`auth to ${user}@${host}:${port} with key '${keyFile}' ...`);
  try {
    const privateKey = fs.readFileSync(getAbsolutePath(keyFile));
    return attemptAuth(client, { host, port, user, privateKey });
  } catch (err) {
    // File doesnt exist
    debug(err.message);
    return false;
  }
};

const getAbsolutePath = (filePath) => {
  if (filePath.substring(0, 2) === "~/") {
    filePath = path.resolve(process.env.HOME, filePath.substring("~/".length));
  }
  if (filePath.substring(0, 1) !== "/") {
    filePath = path.resolve(process.cwd(), filePath);
  }
  return filePath;
};

module.exports = { sshClient, forwardPort };
