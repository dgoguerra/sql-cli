const fs = require("fs");
const path = require("path");
const _ = require("lodash");
const tar = require("tar");
const Knex = require("knex");
const split = require("split2");
const through = require("through2");
const prettier = require("prettier");
const { stringDate } = require("./stringDate");
const { runPipeline } = require("./streamUtils");
const {
  columnInfo,
  listTables,
  listIndexes,
  countRows,
  toKnexType,
  streamInsert,
} = require("./knexUtils");

const MIGRATIONS_TABLE = "dump_knex_migrations";

class Lib {
  constructor({ knex }) {
    if (knex.name === "knex") {
      this.knexIsInternal = false;
      this.knex = knex;
    } else {
      this.knexIsInternal = true;
      this.knex = Knex(knex);
    }
  }

  async checkConnection() {
    await this.knex.raw("select 1+1 as result");
  }

  async destroy() {
    if (this.knexIsInternal) {
      await this.knex.destroy();
    }
  }

  async tableExists(tableName) {
    if (this.knex.client.constructor.name === "Client_BigQuery") {
      return true;
    }
    return await this.knex.schema.hasTable(tableName);
  }

  async getDatabaseSchema() {
    const tables = {};
    for (const table of await this.listTables()) {
      const name = table.table;
      tables[name] = { ...table, schema: await this.getTableSchema(name) };
    }
    return tables;
  }

  async listTables() {
    return listTables(this.knex);
  }

  async getTableSchema(table) {
    return columnInfo(this.knex, table);
  }

  async listIndexes(table) {
    return listIndexes(this.knex, table);
  }

  async createDump(dumpName = this.buildConnSlug("dump")) {
    const dumpDir = `${process.env.PWD}/${dumpName}`;

    fs.rmdirSync(dumpDir, { recursive: true });
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

    for (const { table } of await this.listTables()) {
      if (table.startsWith(MIGRATIONS_TABLE)) {
        continue;
      }

      const columns = await this.knex(table).columnInfo();
      const indexes = await this.listIndexes(table);

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

      const numRows = await countRows(this.knex, table);
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
    fs.rmdirSync(dumpDir, { recursive: true });

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

    for (const filename of fs.readdirSync(`${extractedPath}/data`)) {
      const table = filename.replace(".jsonl", "");
      const stream = fs
        .createReadStream(`${extractedPath}/data/${filename}`)
        .pipe(split())
        .pipe(through.obj((row, enc, next) => next(null, JSON.parse(row))));

      console.log(`Loading data to ${table} ...`);

      await this.knex(table).truncate();
      await streamInsert(this.knex, table, stream);
    }

    fs.rmdirSync(extractedPath, { recursive: true });
  }

  buildConnSlug(prefix = "") {
    const { connection: conn } = this.knex.client.config;
    return _.snakeCase(
      `${prefix}-${conn.server || conn.host}-${conn.database}-${stringDate()}`
    ).replace(/_/g, "-");
  }
}

module.exports = Lib;
