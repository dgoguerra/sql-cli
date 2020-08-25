const fs = require("fs");
const net = require("net");
const getPort = require("get-port");
const { Server } = require("ssh2");
const { sshClient, forwardPort } = require("./sshUtils");

const TEST_USERNAME = "user";
const TEST_PASSWORD = "secret";
const TEST_RSA_KEY = fs.readFileSync(
  `${process.env.PWD}/test/data/test-rsa-key`
);

// Create a fake SSH server
const sshServer = async ({ port, host = "127.0.0.1" }) =>
  new Promise((resolve) => {
    const server = new Server({ hostKeys: [TEST_RSA_KEY] }, (client) => {
      // Authenticate clients with test password credentials
      client.on("authentication", (ctx) => {
        if (
          ctx.method === "password" &&
          ctx.username === TEST_USERNAME &&
          ctx.password === TEST_PASSWORD
        ) {
          return ctx.accept();
        }
        ctx.reject();
      });

      // Client has requested a new session
      client.on("session", (accept, reject) => {
        const session = accept();

        // Mock running a command. Just return a response
        session.on("exec", (accept, reject, info) => {
          const stream = accept();
          stream.write(`ssh: ${info.command} -> pong`);
          stream.exit(0);
          stream.end();
        });
      });

      // Client has requested an outbound connection
      client.on("tcpip", (accept, reject, info) => {
        const srcStream = accept();
        const dstStream = net.createConnection({ port: info.destPort });
        srcStream.pipe(dstStream).pipe(srcStream);
      });
    });

    server.listen(port, host, () => resolve(server));
  });

const runSshCmd = (client, cmd) =>
  new Promise((resolve) => {
    let buffer = "";
    client.exec(cmd, (err, stream) => {
      stream
        .on("data", (data) => (buffer += data))
        .on("close", (code, signal) => resolve(buffer));
    });
  });

const sockServer = (port) =>
  new Promise((resolve) => {
    const server = net.createServer((sock) => {
      sock.on("data", (data) => {
        sock.end(`tcp: ${data} -> pong`);
      });
    });
    server.listen(port, () => resolve(server));
  });

const runTcpCmd = (port, cmd) =>
  new Promise((resolve) => {
    let buffer = "";
    const client = net.createConnection({ port }, () => client.write(cmd));
    client.on("data", (data) => (buffer += data));
    client.on("end", () => resolve(buffer));
  });

describe("sshUtils", () => {
  let port;
  let server;

  beforeEach(async () => {
    port = await getPort();
    server = await sshServer({ port });
  });

  afterEach(async () => {
    await new Promise((resolve, reject) =>
      server.close((err) => (err ? reject(err) : resolve()))
    );
  });

  it("can connect to a ssh server", async () => {
    const client = await sshClient({
      host: "127.0.0.1",
      port,
      user: TEST_USERNAME,
      password: TEST_PASSWORD,
    });

    expect(await runSshCmd(client, "ping")).toBe("ssh: ping -> pong");

    client.end();
  });

  it("can forward port through ssh connection", async () => {
    const client = await sshClient({
      host: "127.0.0.1",
      port,
      user: TEST_USERNAME,
      password: TEST_PASSWORD,
    });

    const tcpPort = await getPort();
    const tcpServer = await sockServer(tcpPort);

    const srcPort = await getPort();
    await forwardPort(client, {
      srcHost: "127.0.0.1",
      srcPort,
      dstHost: "127.0.0.1",
      dstPort: tcpPort,
    });

    expect(await runTcpCmd(srcPort, "ping")).toBe("tcp: ping -> pong");

    tcpServer.close();
    client.end();
  });
});
