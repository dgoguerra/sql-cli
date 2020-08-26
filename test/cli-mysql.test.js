const Knex = require("knex");
const { cliTestSuite } = require("./cli-test-suite");

const TEST_MYSQL_CONN = {
  host: "127.0.0.1",
  port: 23306,
  user: "test",
  password: "Secret123",
  database: "test_db",
  charset: "utf8mb4",
  timezone: "+00:00",
};

cliTestSuite("mysql2", () =>
  Knex({ client: "mysql2", connection: TEST_MYSQL_CONN })
);
