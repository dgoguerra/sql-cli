const _ = require("lodash");
const Knex = require("knex");
const getPort = require("get-port");
const { stringDate } = require("./stringDate");
const { hydrateKnex } = require("./knexUtils");
const { sshClient, forwardPort } = require("./sshUtils");
const { resolveProtocol, getProtocolPort } = require("./connUtils");

class SqlLib extends Function {
  constructor(conn) {
    super();

    this.conn = conn;
    this.knex = null;
    this.sshClient = null;

    return new Proxy(this, {
      apply: (target, thisArg, argArray) => {
        return target.knex.apply(target.knex, argArray);
      },
      get: (target, prop) => {
        return prop in target ? target[prop] : target.knex[prop];
      },
    });
  }

  async connect() {
    if (this.conn.sshHost) {
      await this._setupPortForwarding();
    }

    const knex = this.createKnex();

    this.knex = hydrateKnex(knex);

    await this.checkConnection();

    return this;
  }

  async checkConnection() {
    await this.knex.raw("select 1+1 as result");
    return true;
  }

  async destroy() {
    await this.knex.destroy();

    if (this.sshClient) {
      this.sshClient.destroy();
    }
  }

  buildConnSlug(prefix = "") {
    const { connection: conn } = this.knex.client.config;
    return _.snakeCase(
      `${prefix}-${conn.server || conn.host}-${conn.database}-${stringDate()}`
    ).replace(/_/g, "-");
  }

  async _setupPortForwarding() {
    this.sshClient = await sshClient({
      host: this.conn.sshHost,
      port: this.conn.sshPort,
      user: this.conn.sshUser,
      password: this.conn.sshPassword,
    });

    const freePort = await getPort();
    await forwardPort(this.sshClient, {
      srcHost: "127.0.0.1",
      srcPort: freePort,
      dstHost: this.conn.host,
      dstPort: this.conn.port || getProtocolPort(this.conn.protocol),
    });

    this.conn.host = "127.0.0.1";
    this.conn.port = freePort;
  }

  createKnex() {
    const rest = {
      log: {
        // Fix: avoid overly verbose warning during Knex migrations.
        // See https://github.com/knex/knex/issues/3921
        warn(msg) {
          msg.startsWith("FS-related option") || console.log(msg);
        },
      },
    };

    let conn = this.conn;
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
  }
}

module.exports = SqlLib;
