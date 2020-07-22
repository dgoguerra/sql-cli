const { resolveKnexConn } = require("./resolveKnexConn");

describe("resolveKnexConn()", () => {
  it("mysql conn", () => {
    const [conn] = resolveKnexConn("mysql://app:secret@127.0.0.1:33060/dbname");
    expect(conn).toEqual({
      client: "mysql2",
      connection: {
        charset: "utf8mb4",
        database: "dbname",
        host: "127.0.0.1",
        password: "secret",
        port: "33060",
        timezone: "+00:00",
        user: "app",
      },
    });
  });

  it("mssql conn", () => {
    const [conn] = resolveKnexConn("mssql://app:secret@domain.com:1433/dbname");
    expect(conn).toEqual({
      client: "mssql",
      connection: {
        database: "dbname",
        password: "secret",
        port: 1433,
        server: "domain.com",
        user: "app",
        options: { enableArithAbort: true },
      },
    });
  });

  it("sqlite conn", () => {
    const [conn] = resolveKnexConn("sqlite:///path/to/file/mydb.db");
    expect(conn).toEqual({
      client: "sqlite3",
      connection: {
        filename: "/path/to/file/mydb.db",
      },
      useNullAsDefault: true,
    });
  });

  it("bigquery conn with extra params", () => {
    const [conn] = resolveKnexConn(
      "bq://gcp-project/dataset-name?location=europe-west2&keyFilename=/path/to/service-account.json"
    );
    expect(conn).toEqual({
      client: require("./clients/BigQuery"),
      connection: {
        database: "dataset-name",
        host: "gcp-project",
        keyFilename: "/path/to/service-account.json",
        location: "europe-west2",
        projectId: "gcp-project",
      },
    });
  });

  it("resolve conn alias", () => {
    const [conn] = resolveKnexConn("mydb", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conn).toEqual({
      client: "mysql2",
      connection: {
        charset: "utf8mb4",
        database: "dbname",
        host: "127.0.0.1",
        password: "secret",
        port: "33060",
        timezone: "+00:00",
        user: "app",
      },
    });
  });

  it("conn specifying table", () => {
    const [conn, table] = resolveKnexConn(
      "mysql://app:secret@127.0.0.1:33060/dbname/tablename"
    );
    expect(conn).toMatchObject({
      client: "mysql2",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });

  it("resolve conn alias with table", () => {
    const [conn, table] = resolveKnexConn("mydb/tablename", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conn).toMatchObject({
      client: "mysql2",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });
});
