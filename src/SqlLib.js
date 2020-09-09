const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const tar = require("tar");
const Knex = require("knex");
const split = require("split2");
const rimraf = require("rimraf");
const getPort = require("get-port");
const through = require("through2");
const prettier = require("prettier");
const { stringDate } = require("./stringDate");
const { runPipeline } = require("./streamUtils");
const { sshClient, forwardPort } = require("./sshUtils");
const { hydrateKnex, toKnexType, streamInsert } = require("./knexUtils");

const MIGRATIONS_TABLE = "dump_knex_migrations";

class SqlLib extends Function {
  constructor({ conf, sshConf = null }) {
    super();

    this.knex = null;
    this.conf = conf;
    this.sshConf = sshConf;

    return new Proxy(this, {
      apply: (target, thisArg, argArray) => {
        return target.knex.apply(target.knex, argArray);
      },
      get: (target, prop) => {
        return prop in target ? target[prop] : target.knex[prop];
      },
    });
  }

  async connect() {
    const { connection: conn, ...rest } = this.conf;

    if (this.sshConf) {
      this.sshClient = await sshClient(this.sshConf);

      const freePort = await getPort();
      await forwardPort(this.sshClient, {
        srcHost: "127.0.0.1",
        srcPort: freePort,
        dstHost: conn.host || conn.server,
        dstPort: conn.port,
      });

      // Host may be set in a 'server' property, for example in MSSQL
      conn[conn.host ? "host" : "server"] = "127.0.0.1";
      conn.port = freePort;
    }

    this.knex = hydrateKnex(Knex({ connection: conn, ...rest }));

    await this.checkConnection();

    return this;
  }

  async checkConnection() {
    await this.knex.raw("select 1+1 as result");
    return true;
  }

  async destroy() {
    await this.knex.destroy();

    if (this.sshClient) {
      this.sshClient.destroy();
    }
  }

  async createDump(name) {
    const dumpName = name || this.buildConnSlug("dump");
    const dumpDir = `${process.env.PWD}/${dumpName}`;

    rimraf.sync(dumpDir);
    fs.mkdirSync(`${dumpDir}/data`, { recursive: true });
    fs.mkdirSync(`${dumpDir}/migrations`, { recursive: true });

    const cleanRow = (row) => {
      Object.keys(row).forEach((key) => {
        // Remove empty keys
        if (row[key] === null) {
          delete row[key];
        }
        // Save boolean values as 0 or 1 (avoid MSSQL insert errors)
        if (row[key] === true) {
          row[key] = 1;
        }
        if (row[key] === false) {
          row[key] = 0;
        }
      });
      return row;
    };

    for (const { table } of await this.schema.listTables()) {
      if (table.startsWith(MIGRATIONS_TABLE)) {
        continue;
      }

      const columns = await this(table).columnInfo();
      const indexes = await this.schema.listIndexes(table);

      const primaryKeyTypes = {
        integer: "increments",
        bigInteger: "bigIncrements",
      };

      let primaryKey = "";
      const statements = [];

      const isNumeric = (v) =>
        (typeof v === "number" || typeof v === "string") &&
        Number.isFinite(Number(v));
      const wrapValue = (v) =>
        typeof v === "string" && !isNumeric(v) ? `"${v}"` : v;
      // Depending on the client, default values may be returned as a string
      // wrapped by quotes and/or parenthesis. Ex:
      // default integer 0 -> returned as "('0')"
      // default string "str" -> returned as "'str'"
      const cleanDefault = (v) => {
        if (typeof v !== "string" || isNumeric(v)) {
          return v;
        }
        v = v.replace(/\((.*?)\)/, "$1"); // remove parenthesis
        v = v.replace(/'(.*?)'/, "$1"); // remove ''
        v = v.replace(/"(.*?)"/, "$1"); // remove ""
        return v;
      };

      Object.keys(columns).forEach((key) => {
        const col = columns[key];

        let type = toKnexType(col.type, col.maxLength);

        if (!primaryKey && primaryKeyTypes[type] && !col.nullable) {
          type = primaryKeyTypes[type];
          primaryKey = key;
        }

        const statement = [
          `t.${type}(${wrapValue(key)})`,
          type !== "increments" &&
            type !== "bigIncrements" &&
            !col.nullable &&
            "notNullable()",
          col.defaultValue !== null &&
            type !== "timestamp" &&
            `defaultTo(${wrapValue(cleanDefault(col.defaultValue))})`,
        ];
        statements.push(statement.filter((str) => str).join("."));
      });

      indexes.forEach((index) => {
        // Ignore primary key index, its created while creating the column
        if (
          index.unique &&
          index.columns.length === 1 &&
          index.columns[0] === primaryKey
        ) {
          return;
        }
        const statement = `t.${
          index.unique ? "unique" : "index"
        }([${index.columns.map((c) => wrapValue(c))}], ${wrapValue(
          index.index
        )})`;
        statements.push(statement);
      });

      const migration = `
        module.exports.up = async (knex) => {
          await knex.schema.createTable("${table}", t => {
            ${statements.join(";\n")}
          });
        };
        module.exports.down = async (knex) => {
          await knex.schema.dropTableIfExists("${table}");
        };
      `;

      const migrationPath = `migrations/${stringDate()}-${table}.js`;
      fs.writeFileSync(
        `${dumpDir}/${migrationPath}`,
        prettier.format(migration, { parser: "babel" })
      );
      console.log(`Created ${migrationPath}`);

      const numRows = await this(table).countRows();
      if (numRows > 0) {
        const dataPath = `data/${table}.jsonl`;
        await runPipeline(
          this.knex(table).stream(),
          through.obj((row, enc, next) => {
            next(null, JSON.stringify(cleanRow(row)) + "\n");
          }),
          fs.createWriteStream(`${dumpDir}/${dataPath}`)
        );
        console.log(`Created ${dataPath}`);
      }
    }

    const tarballPath = `${dumpDir}.tgz`;
    await tar.create({ gzip: true, file: tarballPath }, [dumpName]);

    rimraf.sync(dumpDir);

    return tarballPath;
  }

  async loadDump(dump) {
    const dumpPath = path.resolve(dump);

    if (!fs.existsSync(dumpPath)) {
      throw new Error(`Dump file '${dumpPath}' not found`);
    }

    await tar.extract({ file: dumpPath });

    const extractedPath = dumpPath.replace(/\.tgz$/, "");

    if (!fs.existsSync(`${extractedPath}/migrations`)) {
      throw new Error(`Migration files at '${extractedPath}' not found`);
    }

    await this.knex.migrate.latest({
      directory: `${extractedPath}/migrations`,
      tableName: MIGRATIONS_TABLE,
    });

    // Match datetime strings with the formats used by
    // different database drivers:
    // - 2012-01-01 00:00:00
    // - 2012-01-01T00:00:00.000Z (ISO)
    const regexDate = /^\d{4}-\d\d-\d\d( |T)\d\d:\d\d:\d\d/;

    const formatRow = (row) => {
      for (const key in row) {
        if (typeof row[key] === "string" && regexDate.test(row[key])) {
          row[key] = new Date(row[key]);
        }
      }
      return row;
    };

    for (const filename of fs.readdirSync(`${extractedPath}/data`)) {
      const table = filename.replace(".jsonl", "");
      const stream = fs
        .createReadStream(`${extractedPath}/data/${filename}`)
        .pipe(split())
        .pipe(
          through.obj((row, enc, next) =>
            next(null, formatRow(JSON.parse(row)))
          )
        );

      console.log(`Loading data to ${table} ...`);

      await this.knex(table).truncate();
      await streamInsert(this.knex, table, stream);
    }

    rimraf.sync(extractedPath);
  }

  buildConnSlug(prefix = "") {
    const { connection: conn } = this.knex.client.config;
    return _.snakeCase(
      `${prefix}-${conn.server || conn.host}-${conn.database}-${stringDate()}`
    ).replace(/_/g, "-");
  }
}

module.exports = SqlLib;
