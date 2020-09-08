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

const diffColumns = (table1, table2, { showSimilar = false } = {}) => {
  // Column keys in one or both tables
  const allColKeys = _.union(Object.keys(table1), Object.keys(table2));
  const allColumns = allColKeys.map((key) =>
    diffColumnVersions(key, table1[key], table2[key])
  );

  const summary = buildSummary(allColumns, { showSimilar });
  const columns = allColumns.filter(
    (c) => c.status !== "similar" || showSimilar
  );

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

const diffSchemas = (
  tablesBefore,
  tablesAfter,
  { showSimilar = false } = {}
) => {
  // Table names in one or both schemas
  const allTableKeys = _.union(
    Object.keys(tablesBefore),
    Object.keys(tablesAfter)
  );

  const tables = allTableKeys.map((tableKey) =>
    diffTableVersions(tablesBefore[tableKey], tablesAfter[tableKey])
  );
  const summary = buildSummary(tables, { showSimilar });

  return { tables, summary };
};

const diffTableVersions = (table1, table2) => {
  const { summary, columns } = diffColumns(
    (table1 && table1.schema) || {},
    (table2 && table2.schema) || {},
    { showSimilar: true }
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

const buildSummary = (items, { showSimilar }) => {
  const statusColors = {
    created: chalk.green,
    deleted: chalk.red,
    //changed: chalk.yellow,
  };
  const formatText = (status, text) => {
    if (statusColors[status]) {
      return statusColors[status](text);
    }
    return text;
  };

  return _(items)
    .countBy("status")
    .map((num, status) => {
      let text = formatText(status, `${num}x ${status}`);
      if (status === "similar" && !showSimilar) {
        text += " (hidden)";
      }
      return { num, text };
    })
    .orderBy((c) => -c.num)
    .map("text")
    .join(", ");
};

module.exports = { diffColumns, diffSchemas };
