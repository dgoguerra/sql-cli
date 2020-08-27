const Knex = require("knex");
const { cliTestSuite } = require("./cli-test-suite");

const TEST_MSSQL_CONN = {
  server: "127.0.0.1",
  port: 21433,
  user: "test",
  password: "Secret123",
  database: "test_db",
  options: { enableArithAbort: true },
};

cliTestSuite("mssql", () =>
  Knex({ client: "mssql", connection: TEST_MSSQL_CONN })
);
