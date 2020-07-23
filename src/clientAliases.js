const _ = require("lodash");

// Known client aliases (based on aliases in https://github.com/xo/usql)
const CLIENT_ALIASES = {
  mysql2: ["my", "mysql", "maria", "aurora", "mariadb", "percona"],
  pg: ["pgsql", "postgres", "postgresql"],
  sqlite3: ["sq", "file", "sqlite"],
  mssql: ["ms", "sqlserver"],
  bigquery: ["bq"],
};

const getClient = (alias) =>
  _.findKey(CLIENT_ALIASES, (val) => val.includes(alias));

module.exports = { getClient };
