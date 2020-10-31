const _ = require("lodash");
const getPort = require("get-port");
const { stringDate } = require("./stringDate");
const { sshClient, forwardPort } = require("./sshUtils");
const { getProtocolPort } = require("./connUtils");
const { createKnex } = require("./knex/knex");

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

    this.knex = createKnex(this.conn);

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
    const slug = _.snakeCase(
      `${prefix}-${conn.server || conn.host}-${conn.database}`
    );
    return `${slug.slice(0, 30)}-${stringDate()}`.replace(/_/g, "-");
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
}

module.exports = SqlLib;
