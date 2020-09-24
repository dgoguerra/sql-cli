const { resolveKnexConn, stringifyConn } = require("./resolveKnexConn");

describe("resolveKnexConn()", () => {
  it("mysql conn", () => {
    const { conf } = resolveKnexConn(
      "mysql://app:secret@127.0.0.1:33060/dbname"
    );
    expect(conf).toEqual({
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

  it("mysql conn with table", () => {
    const { conf, table } = resolveKnexConn(
      "mysql://app:secret@127.0.0.1:33060/dbname/tablename"
    );
    expect(conf).toMatchObject({
      client: "mysql2",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });

  it("mssql conn", () => {
    const { conf } = resolveKnexConn(
      "mssql://app:secret@domain.com:1433/dbname"
    );
    expect(conf).toEqual({
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

  it("mssql conn with table", () => {
    const { conf, table } = resolveKnexConn(
      "mssql://app:secret@domain.com:1433/dbname/tablename"
    );
    expect(conf).toMatchObject({
      client: "mssql",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });

  it("sqlite conn", () => {
    const { conf } = resolveKnexConn("sqlite:///path/to/file/mydb.db");
    expect(conf).toEqual({
      client: "sqlite3",
      connection: {
        filename: "/path/to/file/mydb.db",
      },
      useNullAsDefault: true,
    });
  });

  it("sqlite conn with table", () => {
    const { conf, table } = resolveKnexConn(
      "sqlite:///path/to/file/mydb.db/tablename"
    );
    expect(conf).toMatchObject({
      client: "sqlite3",
      connection: { filename: "/path/to/file/mydb.db" },
    });
    expect(table).toBe("tablename");
  });

  it("sqlite conn without path, with table", () => {
    const { conf, table } = resolveKnexConn("sqlite://mydb.db/tablename");
    expect(conf).toMatchObject({
      client: "sqlite3",
      connection: { filename: "./mydb.db" },
    });
    expect(table).toBe("tablename");
  });

  it("bigquery conn with extra params", () => {
    const { conf } = resolveKnexConn(
      "bq://gcp-project/dataset-name?location=europe-west2&keyFilename=/path/to/service-account.json"
    );
    expect(conf).toEqual({
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
    const { conf } = resolveKnexConn("mydb", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conf).toEqual({
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
    const { conf, table } = resolveKnexConn(
      "mysql://app:secret@127.0.0.1:33060/dbname/tablename"
    );
    expect(conf).toMatchObject({
      client: "mysql2",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });

  it("resolve conn alias with table", () => {
    const { conf, table } = resolveKnexConn("mydb/tablename", {
      aliases: { mydb: "mysql://app:secret@127.0.0.1:33060/dbname" },
    });
    expect(conf).toMatchObject({
      client: "mysql2",
      connection: { database: "dbname" },
    });
    expect(table).toBe("tablename");
  });

  it("mysql conn through ssh", () => {
    const { sshConf, conf } = resolveKnexConn(
      "mysql+ssh://sshuser@sshserver.com:22/app:secret@dbserver.com:33060/dbname"
    );
    expect(sshConf).toEqual({
      host: "sshserver.com",
      port: "22",
      user: "sshuser",
    });
    expect(conf).toEqual({
      client: "mysql2",
      connection: {
        charset: "utf8mb4",
        database: "dbname",
        host: "dbserver.com",
        password: "secret",
        port: "33060",
        timezone: "+00:00",
        user: "app",
      },
    });
  });
});

describe("stringifyConn()", () => {
  it("sqlite conn", () => {
    const str = stringifyConn({
      protocol: "sqlite",
      path: "/path/to/file/mydb.db",
    });
    expect(str).toBe("sqlite:///path/to/file/mydb.db");
  });

  it("sqlite conn without path", () => {
    const str = stringifyConn({ protocol: "sqlite", path: "mydb.db" });
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
