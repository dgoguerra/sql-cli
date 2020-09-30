const Knex = require("knex");
const { cliTestSuite } = require("./cli-test-suite");
const { stringifyConn } = require("../src/resolveKnexConn");

const TEST_MYSQL_CONN = {
  host: "127.0.0.1",
  port: 23307, // mysql-2 container
  user: "test",
  password: "Secret123",
  database: "test_db",
  charset: "utf8mb4",
  timezone: "+00:00",
};

const TEST_SSH_CONN = {
  host: "127.0.0.1",
  port: 2222,
  user: "user",
  password: "pass",
};

const TEST_SSH_CONN_URI = stringifyConn({
  protocol: "mysql",
  // MySQL host and port as seen from the SSH server
  host: "mysql-2",
  port: 3306,
  user: TEST_MYSQL_CONN.user,
  password: TEST_MYSQL_CONN.password,
  database: TEST_MYSQL_CONN.database,
  sshHost: TEST_SSH_CONN.host,
  sshPort: TEST_SSH_CONN.port,
  sshUser: TEST_SSH_CONN.user,
  sshPassword: TEST_SSH_CONN.password,
});

cliTestSuite(
  "mysql2+ssh",
  () => Knex({ client: "mysql2", connection: TEST_MYSQL_CONN }),
  // Pass a custom connection URI to use in all commands of the suite,
  // to connect to the database through SSH.
  { connUri: TEST_SSH_CONN_URI }
);
