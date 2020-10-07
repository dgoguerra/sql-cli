const Knex = require("knex");
const { cliTestSuite } = require("./cli-test-suite");

const TEST_POSTGRES_CONN = {
  host: "127.0.0.1",
  port: 25432,
  user: "test",
  password: "Secret123",
  database: "test_db",
};

cliTestSuite(
  "pg",
  () => Knex({ client: "pg", connection: TEST_POSTGRES_CONN }),
  { sshHost: "pg", sshPort: 5432 }
);
