const { parseUri, resolveConn, stringifyConn } = require("./connUtils");

describe("parseUri()", () => {
  it("mysql conn", () => {
    const parsed = parseUri("mysql://app:secret@127.0.0.1:33060/dbname");
    expect(parsed).toMatchObject({
      protocol: "mysql",
      user: "app",
      password: "secret",
      host: "127.0.0.1",
      port: 33060,
      path: "/dbname",
    });
  });

  it("mysql conn with table", () => {
    const parsed = parseUri(
      "mysql://app:secret@127.0.0.1:33060/dbname/tablename"
    );
    expect(parsed).toMatchObject({
      protocol: "mysql",
      path: "/dbname/tablename",
    });
  });

  it("mssql conn", () => {
    const parsed = parseUri("mssql://app:secret@domain.com:1433/dbname");
    expect(parsed).toMatchObject({
      protocol: "mssql",
      user: "app",
      password: "secret",
      host: "domain.com",
      port: 1433,
      path: "/dbname",
    });
  });

  it("mssql conn with table", () => {
    const parsed = parseUri(
      "mssql://app:secret@domain.com:1433/dbname/tablename"
    );
    expect(parsed).toMatchObject({
      protocol: "mssql",
      path: "/dbname/tablename",
    });
  });

  it("bigquery conn with extra params", () => {
    const parsed = parseUri(
      "bq://gcp-project/dataset-name?location=europe-west2&keyFilename=/path/to/service-account.json"
    );
    expect(parsed).toMatchObject({
      protocol: "bq",
      host: "gcp-project",
      path: "/dataset-name",
      params: {
        keyFilename: "/path/to/service-account.json",
        location: "europe-west2",
      },
    });
  });

  it("mysql conn through ssh", () => {
    const parsed = parseUri(
      "mysql+ssh://sshuser@sshserver.com:22/app:secret@dbserver.com:33060/dbname"
    );
    expect(parsed).toMatchObject({
      protocol: "mysql+ssh",
      user: "sshuser",
      host: "sshserver.com",
      port: 22,
      path: "/app:secret@dbserver.com:33060/dbname",
    });

    const sshParsed = parseUri(parsed.path.replace(/^\//, ""));
    expect(sshParsed).toMatchObject({
      user: "app",
      password: "secret",
      host: "dbserver.com",
      port: 33060,
      path: "/dbname",
    });
  });

  it("sqlite conn with absolute path", () => {
    const parsed = parseUri("sqlite:///path/to/file/mydb.db");
    expect(parsed).toMatchObject({
      protocol: "sqlite",
      host: "",
      path: "/path/to/file/mydb.db",
    });
  });

  it("sqlite conn with relative path, with table", () => {
    const parsed = parseUri("sqlite://path/to/db/tablename");
    expect(parsed).toMatchObject({
      protocol: "sqlite",
      host: "path",
      path: "/to/db/tablename",
    });
  });

  it("conn alias", () => {
    const parsed = parseUri("mydb");
    expect(parsed).toMatchObject({ host: "mydb" });
  });

  it("conn alias with table", () => {
    const parsed = parseUri("mydb/tablename");
    expect(parsed).toMatchObject({ host: "mydb", path: "/tablename" });
  });
});

describe("resolveConn()", () => {
  it("mysql conn", () => {
    const conn = resolveConn("mysql://app:secret@127.0.0.1:33060/dbname");
    expect(conn).toMatchObject({
      protocol: "mysql",
      database: "dbname",
      host: "127.0.0.1",
      password: "secret",
      port: 33060,
      user: "app",
    });
  });

  it("mysql conn with table", () => {
    const conn = resolveConn(
      "mysql://app:secret@127.0.0.1:33060/dbname/tablename"
    );
    expect(conn).toMatchObject({
      protocol: "mysql",
      database: "dbname",
      _table: "tablename",
    });
  });

  it("mssql conn", () => {
    const conn = resolveConn("mssql://app:secret@domain.com:1433/dbname");
    expect(conn).toMatchObject({
      protocol: "mssql",
      user: "app",
      password: "secret",
      host: "domain.com",
      port: 1433,
      database: "dbname",
    });
  });

  it("mssql conn with table", () => {
    const conn = resolveConn(
      "mssql://app:secret@domain.com:1433/dbname/tablename"
    );
    expect(conn).toMatchObject({
      protocol: "mssql",
      database: "dbname",
      _table: "tablename",
    });
  });

  it("sqlite conn", () => {
    const conn = resolveConn("sqlite:///path/to/file/mydb.db");
    expect(conn).toMatchObject({
      protocol: "sqlite",
      filename: "/path/to/file/mydb.db",
    });
  });

  it("sqlite conn with table", () => {
    const conn = resolveConn("sqlite:///path/to/file/mydb.db/tablename");
    expect(conn).toMatchObject({
      protocol: "sqlite",
      filename: "/path/to/file/mydb.db",
      _table: "tablename",
    });
  });

  it("sqlite conn without path, with table", () => {
    const conn = resolveConn("sqlite://mydb.db/tablename");
    expect(conn).toMatchObject({
      protocol: "sqlite",
      filename: "mydb.db",
      _table: "tablename",
    });
  });

  it("bigquery conn with extra params", () => {
    const conn = resolveConn(
      "bq://gcp-project/dataset-name?location=europe-west2&keyFilename=/path/to/service-account.json"
    );
    expect(conn).toMatchObject({
      protocol: "bq",
      host: "gcp-project",
      database: "dataset-name",
      params: {
        keyFilename: "/path/to/service-account.json",
        location: "europe-west2",
      },
    });
  });

  it("resolve conn alias", () => {
    const conn = resolveConn("mydb", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conn).toMatchObject({
      protocol: "mysql",
      database: "dbname",
      host: "127.0.0.1",
      password: "secret",
      port: 33060,
      user: "app",
    });
  });

  it("resolve conn alias with table", () => {
    const conn = resolveConn("mydb/tablename", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conn).toMatchObject({
      protocol: "mysql",
      database: "dbname",
      _table: "tablename",
    });
  });

  it("mysql conn through ssh", () => {
    const conn = resolveConn(
      "mysql+ssh://sshuser@sshserver.com:22/app:secret@dbserver.com:33060/dbname"
    );
    expect(conn).toMatchObject({
      protocol: "mysql",
      database: "dbname",
      host: "dbserver.com",
      password: "secret",
      port: 33060,
      user: "app",
      sshHost: "sshserver.com",
      sshPort: 22,
      sshUser: "sshuser",
    });
  });
});

describe("stringifyConn()", () => {
  it("sqlite conn", () => {
    const str = stringifyConn({
      protocol: "sqlite",
      filename: "/path/to/file/mydb.db",
    });
    expect(str).toBe("sqlite:///path/to/file/mydb.db");
  });

  it("sqlite conn without path", () => {
    const str = stringifyConn({ protocol: "sqlite", filename: "mydb.db" });
    expect(str).toBe("sqlite://mydb.db");
  });

  it("mysql conn", () => {
    const str = stringifyConn({
      protocol: "mysql2",
      user: "app",
      password: "secret",
      host: "127.0.0.1",
      port: 33060,
      database: "dbname",
    });
    expect(str).toBe("mysql2://app:secret@127.0.0.1:33060/dbname");
  });

  it("mssql conn over ssh", () => {
    const str = stringifyConn({
      protocol: "mssql",
      user: "app",
      password: "secret",
      host: "127.0.0.1",
      port: 1433,
      database: "dbname",
      sshUser: "sshuser",
      sshPassword: "sshsecret",
      sshHost: "123.123.123.123",
      sshPort: 2222,
    });
    expect(str).toBe(
      "mssql+ssh://sshuser:sshsecret@123.123.123.123:2222/app:secret@127.0.0.1:1433/dbname"
    );
  });
});
