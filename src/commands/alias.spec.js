const { getTestKnex, runCli } = require("../../test/utils");

describe("alias", () => {
  let knex;
  let connUri;

  beforeAll(async () => {
    knex = getTestKnex();
    const { client, connection } = knex.client.config;
    connUri = `${client}://${connection.filename}`;

    await knex.schema.createTable("mytable", (t) => {
      t.increments("id");
    });
  });

  it("can create conn aliases", async () => {
    await runCli(`alias add myalias ${connUri}`);
    expect(await runCli("alias ls")).toBe(
      "alias   conn\n" + `myalias ${connUri}\n`
    );
  });

  it("cannot overwrite an existing alias", async () => {
    await expect(runCli(`alias add myalias ${connUri}`)).rejects.toThrow(
      "Error: Alias 'myalias' already exists"
    );
  });

  it("can use a created alias", async () => {
    expect(await runCli("ls myalias")).toMatchInlineSnapshot(`
      "table   rows bytes
      mytable 0    4.1 kB

      (4.1 kB in 1 tables)
      "
    `);
  });

  it("can delete an alias", async () => {
    await runCli("alias rm myalias");

    await expect(runCli(`alias rm myalias`)).rejects.toThrow(
      "Error: Alias 'myalias' not found"
    );
  });
});
