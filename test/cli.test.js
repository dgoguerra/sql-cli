const { runCli, getTestKnex, getKnexUri } = require("./utils");

describe("CLI basic commands", () => {
  let knex;
  let connUri;

  beforeAll(async () => {
    knex = getTestKnex();
    connUri = getKnexUri(knex);

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.integer("field_1");
      t.text("field_2");
    });
    await knex.schema.createTable("table_2", (t) => {
      t.increments("id");
      t.decimal("field_1");
      t.text("field_2");
    });
    await knex.schema.createTable("table_3", (t) => {
      t.bigIncrements("idField");
      t.bigInteger("field_1");
    });
    await knex("table_1").insert([
      { field_1: 12, field_2: "foo" },
      { field_1: 30, field_2: "bar" },
    ]);
    await knex("table_2").insert([
      { field_1: 12.3, field_2: "foo" },
      { field_1: 30.45, field_2: "bar" },
    ]);
  });

  it("can list tables", async () => {
    expect(await runCli("list", [connUri])).toMatchSnapshot();
  });

  it("can show table", async () => {
    expect(await runCli("show", [`${connUri}/table_2`])).toMatchSnapshot();
  });

  it("can diff tables", async () => {
    expect(
      await runCli("diff", [`${connUri}/table_1`, `${connUri}/table_2`])
    ).toMatchSnapshot();
  });

  it("can diff schemas", async () => {
    expect(await runCli("diff", [connUri, connUri])).toMatchSnapshot();
  });

  it("can create conn aliases", async () => {
    await runCli("alias", [
      "add",
      "alias1",
      "mysql://app:secret@127.0.0.1:1234/dbname",
    ]);
    await runCli("alias", ["add", "alias2", "sqlite3:///path/to/file.db"]);

    // Alias has been created
    expect(await runCli("alias", ["ls"])).toMatchSnapshot();
  });

  it("can use a created alias", async () => {
    await runCli("alias", ["add", "test-alias", connUri]);
    expect(await runCli("list", ["test-alias"])).toMatchSnapshot();
  });

  it("can delete conn aliases", async () => {
    await runCli("alias", ["rm", "test-alias"]);
    await runCli("alias", ["rm", "alias1"]);

    // Only alias2 still exists
    expect(await runCli("alias", ["ls"])).toMatchSnapshot();

    await runCli("alias", ["rm", "alias2"]);
  });
});
