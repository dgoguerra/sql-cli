const _ = require("lodash");
const chalk = require("chalk");

const colHash = (col) => `${col.fullType}:${col.nullable}`;

const colDesc = (col) => {
  let str = col.fullType;
  if (col.nullable) {
    str += " nullable";
  }
  return str;
};

const valueOrDiff = (before, after) =>
  before === after ? before : `${chalk.red(before)} → ${chalk.green(after)}`;

const diffColumns = (table1, table2) => {
  // Merge unique column keys of both tables, to show all
  // columns in one or both of them.
  const allColumnKeys = _.union(Object.keys(table1), Object.keys(table2));

  const columns = allColumnKeys.map((key) =>
    diffColumnVersions(key, table1[key] || null, table2[key] || null)
  );

  const summary = _(columns)
    .countBy("status")
    .map((num, status) => {
      const statusColors = {
        created: chalk.green,
        deleted: chalk.red,
        //changed: chalk.yellow,
      };
      const color = statusColors[status] || ((val) => val);
      return { num, text: color(`${num}x ${status}`) };
    })
    .orderBy((c) => -c.num)
    .map("text")
    .join(", ");

  return { columns, summary };
};

const diffColumnVersions = (key, col1, col2) => {
  const desc1 = col1 && colDesc(col1);
  const desc2 = col2 && colDesc(col2);

  if (!col2) {
    return {
      status: "deleted",
      displayColumn: chalk.red(key),
      displayType: chalk.red(desc1),
    };
  }

  if (!col1) {
    return {
      status: "created",
      displayColumn: chalk.green(key),
      displayType: chalk.green(desc2),
    };
  }

  const changed = colHash(col1) !== colHash(col2);
  return {
    status: changed ? "changed" : "similar",
    displayColumn: key,
    displayType: valueOrDiff(desc1, desc2),
  };
};

const diffSchemas = (tablesBefore, tablesAfter) => {
  const allTableKeys = _.union(
    Object.keys(tablesBefore),
    Object.keys(tablesAfter)
  );

  const tables = allTableKeys.map((tableKey) =>
    diffTableVersions(
      tablesBefore[tableKey] || null,
      tablesAfter[tableKey] || null
    )
  );

  const summary = _(tables)
    .countBy("status")
    .map((num, status) => {
      const statusColors = {
        created: chalk.green,
        deleted: chalk.red,
        //changed: chalk.yellow,
      };
      const color = statusColors[status] || ((val) => val);
      return { num, text: color(`${num}x ${status}`) };
    })
    .orderBy((c) => -c.num)
    .map("text")
    .join(", ");

  return { tables, summary };
};

const diffTableVersions = (table1, table2) => {
  const { summary, columns } = diffColumns(
    (table1 && table1.schema) || {},
    (table2 && table2.schema) || {}
  );

  if (!table1) {
    return {
      status: "created",
      displayTable: chalk.green(table2.table),
      displayRows: chalk.green(table2.rows),
      displayBytes: chalk.green(table2.prettyBytes),
      summary,
    };
  }

  if (!table2) {
    return {
      status: "deleted",
      displayTable: chalk.red(table1.table),
      displayRows: chalk.red(table1.rows),
      displayBytes: chalk.red(table1.prettyBytes),
      summary,
    };
  }

  const columnsChanged = columns.filter((c) => c.status !== "similar");

  const areEqual =
    !columnsChanged.length &&
    table1.rows === table2.rows &&
    table1.prettyBytes === table2.prettyBytes;

  return {
    status: areEqual ? "similar" : "changed",
    displayTable: table1.table,
    displayRows: valueOrDiff(table1.rows, table2.rows),
    displayBytes: valueOrDiff(table1.prettyBytes, table2.prettyBytes),
    summary,
  };
};

module.exports = { diffColumns, diffSchemas };
