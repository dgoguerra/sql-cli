const fs = require("fs");
const path = require("path");
const tar = require("tar");
const _ = require("lodash");
const split = require("split2");
const rimraf = require("rimraf");
const through = require("through2");
const prettier = require("prettier");
const { stringDate } = require("./stringDate");
const { runPipeline } = require("./streamUtils");
const { toKnexType, streamInsert } = require("./knexUtils");

const isNumeric = (v) =>
  (typeof v === "number" || typeof v === "string") &&
  Number.isFinite(Number(v));

const wrapValue = (v) =>
  typeof v === "string" && !isNumeric(v) ? `"${v}"` : v;

class SqlDumper {
  constructor(knex, { dumpsDir = process.env.PWD } = {}) {
    this.knex = knex;
    this.dumpsDir = dumpsDir;
  }

  async createDump(name = null) {
    const dumpName = name
      ? name.replace(/\.tgz$/, "")
      : this.knex.buildConnSlug("dump");

    const dumpDir = path.isAbsolute(dumpName)
      ? dumpName
      : path.resolve(this.dumpsDir, dumpName);

    rimraf.sync(dumpDir);
    fs.mkdirSync(`${dumpDir}/data`, { recursive: true });
    fs.mkdirSync(`${dumpDir}/migrations`, { recursive: true });

    for (const { table } of await this.knex.schema.listTables()) {
      const migrPath = `migrations/${stringDate()}-${table}.js`;
      const dataPath = `data/${table}.jsonl`;

      if (await this.createTableMigration(table, `${dumpDir}/${migrPath}`)) {
        console.log(`Created ${migrPath}`);
      }

      if (await this.createTableContent(table, `${dumpDir}/${dataPath}`)) {
        console.log(`Created ${dataPath}`);
      }
    }

    const tarballPath = `${dumpDir}.tgz`;
    await tar.create(
      { gzip: true, file: tarballPath, cwd: path.dirname(dumpDir) },
      [path.basename(dumpDir)]
    );

    rimraf.sync(dumpDir);

    return tarballPath;
  }

  async loadDump(dump) {
    const dumpPath = path.resolve(dump);
    const dumpName = path.basename(dump).replace(/.tgz$/, "");
    const dumpSlug = _.snakeCase(dumpName).replace(/-/g, "_");

    if (!fs.existsSync(dumpPath)) {
      throw new Error(`Dump file '${dumpPath}' not found`);
    }

    await tar.extract({ file: dumpPath });

    const extractedPath = dumpPath.replace(/\.tgz$/, "");

    if (!fs.existsSync(`${extractedPath}/migrations`)) {
      throw new Error(`Migration files at '${extractedPath}' not found`);
    }

    const tableName = `migrations_${dumpSlug}`;

    console.log(`Saving migrations to table ${tableName} ...`);
    await this.knex.migrate.latest({
      directory: `${extractedPath}/migrations`,
      tableName,
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

  async createTableMigration(table, filePath) {
    const primaryKey = await this.knex(table).getPrimaryKey();
    const columns = await this.knex(table).columnInfo();
    const indexes = await this.knex.schema.listIndexes(table);

    const statements = [];

    Object.keys(columns).forEach((key) => {
      statements.push(
        this.buildColumnStatement(key, columns[key], {
          primaryKey: key === primaryKey,
        })
      );
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
      statements.push(this.buildIndexStatement(index));
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

    fs.writeFileSync(filePath, prettier.format(migration, { parser: "babel" }));

    return true;
  }

  async createTableContent(table, filePath) {
    const numRows = await this.knex(table).countRows();

    if (!numRows) {
      return false;
    }

    await runPipeline(
      this.knex(table).stream(),
      through.obj((row, enc, next) => {
        next(null, JSON.stringify(this.cleanRow(row)) + "\n");
      }),
      fs.createWriteStream(filePath)
    );
    return true;
  }

  buildColumnStatement(key, col, { primaryKey = false } = {}) {
    const primaryKeyTypes = {
      integer: "increments",
      bigInteger: "bigIncrements",
    };

    let type = toKnexType(col.type, col.maxLength);

    if (primaryKey) {
      type = primaryKeyTypes[type] || type;
    }

    const statement = [`t.${type}(${wrapValue(key)})`];

    if (!primaryKey && !col.nullable) {
      statement.push("notNullable()");
    }

    if (col.defaultValue !== null && type !== "timestamp") {
      const value = this.cleanDefaultValue(col.defaultValue);
      statement.push(`defaultTo(${wrapValue(value)})`);
    }

    return statement.join(".");
  }

  buildIndexStatement(index) {
    const type = index.unique ? "unique" : "index";
    const columns = index.columns.map((c) => wrapValue(c));
    return `t.${type}([${columns}], ${wrapValue(index.name)})`;
  }

  // Depending on the client, default values may be returned as a string
  // wrapped by quotes and/or parenthesis. Ex:
  // default integer 0 -> returned as "('0')"
  // default string "str" -> returned as "'str'"
  cleanDefaultValue(val) {
    if (typeof val !== "string" || isNumeric(val)) {
      return val;
    }
    val = val.replace(/\((.*?)\)/, "$1"); // remove parenthesis
    val = val.replace(/'(.*?)'/, "$1"); // remove ''
    val = val.replace(/"(.*?)"/, "$1"); // remove ""
    return val;
  }

  cleanRow(row) {
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
  }
}

module.exports = SqlDumper;
