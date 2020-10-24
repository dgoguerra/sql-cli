const fs = require("fs");
const os = require("os");
const path = require("path");
const tar = require("tar");
const _ = require("lodash");
const split = require("split2");
const rimraf = require("rimraf");
const through = require("through2");
const deepEqual = require("deep-equal");
const { EventEmitter } = require("events");
const TableBuilder = require("knex/lib/schema/tablebuilder");
const { stringDate } = require("./stringDate");
const { runPipeline } = require("./streamUtils");
const { toKnexType, streamInsert } = require("./knexUtils");

const isNumeric = (v) =>
  (typeof v === "number" || typeof v === "string") &&
  Number.isFinite(Number(v));

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
    const tarballPath = `${dumpDir}.tgz`;

    if (fs.existsSync(tarballPath)) {
      throw new Error(`Dump file '${tarballPath}' already exists`);
    }

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
    const dateRegex = /^\d{4}-\d\d-\d\d( |T)\d\d:\d\d:\d\d/;

    const formatRow = (row) => {
      for (const key in row) {
        // Convert date strings to Date
        if (typeof row[key] === "string" && dateRegex.test(row[key])) {
          row[key] = new Date(row[key]);
        }
        // Stringify JSON objects (some drivers return fields of type 'json'
        // directly as an object when creating the dump data file).
        if (
          row[key] &&
          typeof row[key] === "object" &&
          !(row[key] instanceof Date)
        ) {
          row[key] = JSON.stringify(row[key]);
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

      await this.knex(table).delete();
      await streamInsert(this.knex, table, stream);
    }

    rimraf.sync(extractedPath);
  }

  async createTableMigration(table, filePath) {
    const primaryKey = await this.knex(table).getPrimaryKey();
    const columns = await this.knex.schema.listColumns(table);
    const indexes = await this.knex.schema.listIndexes(table);

    const statements = [];

    columns.forEach((col) => {
      const isPrimaryKey =
        primaryKey.length === 1 && primaryKey[0] === col.name;
      statements.push(this.buildColumnStatement(col, { isPrimaryKey }));
    });

    if (primaryKey.length > 1) {
      statements.push(this.buildPrimaryStatement(primaryKey));
    }

    indexes.forEach((index) => {
      // Ignore primary key index, its created while creating the column
      if (index.unique && deepEqual(primaryKey, index.columns)) {
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

  buildColumnStatement(col, { isPrimaryKey = false } = {}) {
    const primaryKeyTypes = {
      integer: "increments",
      bigInteger: "bigIncrements",
    };

    let type = toKnexType(col.type, col) || col.type;
    let isIncrement = false;

    if (isPrimaryKey && primaryKeyTypes[type]) {
      type = primaryKeyTypes[type];
      isIncrement = true;
    }

    if (!TableBuilder.prototype[type]) {
      throw new Error(
        `Unknown column type '${type}'. Cannot convert it to a known Knex type`
      );
    }

    const statement = [];

    if (col.precision && col.scale) {
      statement.push(
        `t.${type}("${col.name}", ${col.precision}, ${col.scale})`
      );
    } else if (col.maxLength) {
      statement.push(`t.${type}("${col.name}", ${col.maxLength})`);
    } else {
      statement.push(`t.${type}("${col.name}")`);
    }

    // In primary key columns of type "increments" dont apply primary,
    // unsigned, nullable or default properties.
    if (isIncrement) {
      return statement.join(".");
    }

    if (col.unsigned) {
      statement.push("unsigned()");
    }
    // Mark column explicitly as primary key
    if (isPrimaryKey) {
      statement.push("primary()");
    }
    // Set nullable timestamps explicitly (even though Knex makes columns
    // nullable by default). Prevents older versions of MySQL (ex. 5.5)
    // from adding by default "not null default to CURRENT_TIMESTAMP".
    // to timestamp columns.
    if (col.nullable && type === "timestamp") {
      statement.push("nullable()");
    }
    // primary() implies notNullable(), so do not add it
    if (!col.nullable && !isPrimaryKey) {
      statement.push("notNullable()");
    }
    if (col.default !== null) {
      statement.push(this.buildDefaultTo(type, col.default));
    }
    if (col.foreign) {
      statement.push(`references("${col.foreign}")`);
    }

    return statement.join(".");
  }

  buildDefaultTo(type, value) {
    // CURRENT_TIMESTAMP is understood by all databases, but
    // MSSQL replaces it for getdate().
    const nowConstraints = ["CURRENT_TIMESTAMP", "GETDATE()"];

    if (
      (type === "timestamp" || type === "datetime") &&
      nowConstraints.includes(value.toUpperCase())
    ) {
      value = `knex.raw("CURRENT_TIMESTAMP")`;
    } else if (typeof value === "string" && !isNumeric(value)) {
      value = `"${value}"`;
    }

    return `defaultTo(${value})`;
  }

  buildPrimaryStatement(columns) {
    return `t.primary(${JSON.stringify(columns)})`;
  }

  buildIndexStatement(index) {
    const client = this.knex.client.constructor.name;
    const type = index.unique ? "unique" : "index";

    // In MySQL primary key indexes are always called "PRIMARY". If the primary
    // key is composed of several fields, it will be dumped as an index statement,
    // but it cannot be saved with the "PRIMARY" name, since other databases don't
    // allow creating several indexes with the same name.
    const ignoreName =
      (client === "Client_MySQL" || client === "Client_MySQL2") &&
      index.name === "PRIMARY";

    const columns = JSON.stringify(index.columns);
    return ignoreName
      ? `t.${type}(${columns})`
      : `t.${type}(${columns}, "${index.name}")`;
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
