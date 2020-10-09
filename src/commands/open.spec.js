const { runCli } = require("../../test/utils");

describe("open", () => {
  it("can build sqlite open command", async () => {
    expect(await runCli("open sqlite:///path/to/file.db --dry-run"))
      .toMatchInlineSnapshot(`
      "Opening /path/to/file.db ...
      open /path/to/file.db
      "
    `);
  });

  it("can open mysql conn", async () => {
    expect(
      await runCli("open mysql://user:pass@db.com:3306/database --dry-run")
    ).toMatchInlineSnapshot(`
      "Opening mysql://user@db.com:3306/database ...
      open mysql://user:pass@db.com:3306/database
      "
    `);
  });

  it("can open mssql conn", async () => {
    expect(
      await runCli("open mssql://user:pass@db.com:1433/database --dry-run")
    ).toMatchInlineSnapshot(`
      "Opening sqlserver://user@db.com:1433/database ...
      open sqlserver://user:pass@db.com:1433/database
      "
    `);
  });

  it("can open pg conn", async () => {
    expect(await runCli("open pg://user:pass@db.com:5432/database --dry-run"))
      .toMatchInlineSnapshot(`
      "Opening postgres://user@db.com:5432/database ...
      open postgres://user:pass@db.com:5432/database
      "
    `);
  });

  it("can open mysql conn through ssh", async () => {
    expect(
      await runCli(
        "open my+ssh://sshuser@ssh.com:22/db.com:3306/database --dry-run"
      )
    ).toMatchInlineSnapshot(`
      "Opening mysql+ssh://sshuser@ssh.com:22/db.com:3306/database?usePrivateKey=true ...
      open mysql+ssh://sshuser@ssh.com:22/db.com:3306/database?usePrivateKey=true
      "
    `);
  });
});
