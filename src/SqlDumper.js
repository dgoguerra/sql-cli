const fs = require("fs");
const os = require("os");
const path = require("path");
const tar = require("tar");
const _ = require("lodash");
const split = require("split2");
const rimraf = require("rimraf");
const through = require("through2");
const { EventEmitter } = require("events");
const { stringDate } = require("./stringDate");
const { runPipeline } = require("./streamUtils");
const { toKnexType, streamInsert } = require("./knexUtils");

const isNumeric = (v) =>
  (typeof v === "number" || typeof v === "string") &&
  Number.isFinite(Number(v));

const wrapValue = (v) =>
  typeof v === "string" && !isNumeric(v) ? `"${v}"` : v;

class SqlDumper extends EventEmitter {
  constructor(knex, { dumpsDir = process.env.PWD } = {}) {
    super();
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
        this.emit("log", `Created ${migrPath}`);
      }

      if (await this.createTableContent(table, `${dumpDir}/${dataPath}`)) {
        this.emit("log", `Created ${dataPath}`);
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
    const extractedPath = `${os.tmpdir()}/${dumpName}`;

    if (!fs.existsSync(dumpPath)) {
      throw new Error(`Dump file '${dumpPath}' not found`);
    }

    if (fs.existsSync(extractedPath)) {
      rimraf.sync(extractedPath);
    }

    await tar.extract({ file: dumpPath, cwd: path.dirname(extractedPath) });

    if (!fs.existsSync(`${extractedPath}/migrations`)) {
      throw new Error(`Migration files at '${extractedPath}' not found`);
    }

    const tableName = `migrations_${dumpSlug}`;

    this.emit("log", `Saving migrations to table ${tableName} ...`);
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

      this.emit("log", `Loading data to ${table} ...`);

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
        await knex.schema.createTable("${table}", (t) => {
          ${statements.join(";\n    ") + ";"}
        });
      };
      module.exports.down = async (knex) => {
        await knex.schema.dropTableIfExists("${table}");
      };`;

    fs.writeFileSync(
      filePath,
      // Clean up indentation of the template string
      migration.trim().replace(/\n      /g, "\n") + "\n"
    );

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

    const statement = [
      col.maxLength
        ? `t.${type}(${wrapValue(key)}, ${col.maxLength})`
        : `t.${type}(${wrapValue(key)})`,
    ];

    // Dont apply nullable or default value modifiers on primary keys
    if (!primaryKey) {
      // Set nullable timestamps explicitly (even though Knex makes columns
      // nullable by default). Prevents older versions of MySQL (ex. 5.5)
      // from creating the timestamp column as not null, with default
      // value CURRENT_TIMESTAMP.
      if (col.nullable && type === "timestamp") {
        statement.push("nullable()");
      }
      if (!col.nullable) {
        statement.push("notNullable()");
      }
      if (col.defaultValue !== null) {
        statement.push(`defaultTo(${wrapValue(col.defaultValue)})`);
      }
    }

    return statement.join(".");
  }

  buildIndexStatement(index) {
    const client = this.knex.client.constructor.name;
    const type = index.unique ? "unique" : "index";
    const columns = index.columns.map((c) => wrapValue(c));

    // In MySQL primary key indexes are always called "PRIMARY". If the primary
    // key is composed of several fields, it will be dumped as an index statement,
    // but it cannot be saved with the "PRIMARY" name, since other databases don't
    // allow creating several indexes with the same name.
    const ignoreName =
      (client === "Client_MySQL" || client === "Client_MySQL2") &&
      index.name === "PRIMARY";

    return ignoreName
      ? `t.${type}([${columns}])`
      : `t.${type}([${columns}], ${wrapValue(index.name)})`;
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
