const { getTestKnex } = require("./utils");
const { cliTestSuite } = require("./cli-test-suite");

cliTestSuite("sqlite3", () => getTestKnex());
