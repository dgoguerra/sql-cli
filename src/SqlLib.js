const _ = require("lodash");
const Knex = require("knex");
const getPort = require("get-port");
const { stringDate } = require("./stringDate");
const { hydrateKnex } = require("./knexUtils");
const { sshClient, forwardPort } = require("./sshUtils");

class SqlLib extends Function {
  constructor({ conf, sshConf = null }) {
    super();

    this.knex = null;
    this.conf = conf;
    this.sshConf = sshConf;

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
    const { connection: conn, ...rest } = this.conf;

    if (this.sshConf) {
      this.sshClient = await sshClient(this.sshConf);

      const freePort = await getPort();
      await forwardPort(this.sshClient, {
        srcHost: "127.0.0.1",
        srcPort: freePort,
        dstHost: conn.host || conn.server,
        dstPort: conn.port,
      });

      // Host may be set in a 'server' property, for example in MSSQL
      conn[conn.host ? "host" : "server"] = "127.0.0.1";
      conn.port = freePort;
    }

    this.knex = hydrateKnex(Knex({ connection: conn, ...rest }));

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
}

module.exports = SqlLib;
