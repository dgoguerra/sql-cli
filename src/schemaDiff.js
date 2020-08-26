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

const diffColumns = (tableBefore, tableAfter) => {
  const columns = _(Object.keys(tableBefore))
    // Merge unique column keys of before and after schemas,
    // to show any columns seen in one or both schemas.
    .concat(Object.keys(tableAfter))
    .uniq()
    .map((key) => {
      const before = tableBefore[key] || null;
      const after = tableAfter[key] || null;

      const colInfo = {
        column: key,
        descBefore: (before && colDesc(before)) || null,
        descAfter: (after && colDesc(after)) || null,
      };

      if (!after) {
        return { ...colInfo, status: "deleted" };
      }
      if (!before) {
        return { ...colInfo, status: "created" };
      }

      const changed = colHash(before) !== colHash(after);
      return {
        ...colInfo,
        status: changed ? "changed" : "similar",
      };
    })
    .map((col) => {
      switch (col.status) {
        case "deleted":
          col.displayColumn = chalk.red(col.column);
          col.displayType = chalk.red(col.descBefore);
          break;
        case "created":
          col.displayColumn = chalk.green(col.column);
          col.displayType = chalk.green(col.descAfter);
          break;
        case "changed":
        case "similar":
          col.displayColumn = col.column;
          col.displayType = valueOrDiff(col.descBefore, col.descAfter);
          break;
      }
      return col;
    })
    .value();

  const summary = _(columns)
    .countBy("status")
    .map((num, status) => {
      const color =
        status === "created"
          ? chalk.green
          : status === "deleted"
          ? chalk.red
          : (val) => val;
      return { num, text: color(`${num}x ${status}`) };
    })
    .orderBy((c) => -c.num)
    .map("text")
    .join(", ");

  return { columns, summary };
};

module.exports.diffColumns = diffColumns;

const diffSchemas = (tablesBefore, tablesAfter) => {
  return _(Object.keys(tablesBefore))
    .concat(Object.keys(tablesAfter))
    .uniq()
    .map((tableKey) => {
      const before = tablesBefore[tableKey] || null;
      const after = tablesAfter[tableKey] || null;

      const tableInfo = {
        table: tableKey,
        bytesBefore: before && before.prettyBytes,
        bytesAfter: after && after.prettyBytes,
        rowsBefore: before && before.rows,
        rowsAfter: after && after.rows,
      };

      if (!before) {
        return { ...tableInfo, status: "created" };
      }
      if (!after) {
        return { ...tableInfo, status: "deleted" };
      }
      return tableInfo;
    })
    .map((table) => {
      const tableBefore = tablesBefore[table.table];
      const tableAfter = tablesAfter[table.table];

      const { summary } = diffColumns(
        (tableBefore && tableBefore.schema) || {},
        (tableAfter && tableAfter.schema) || {}
      );
      table.displaySummary = summary;

      switch (table.status) {
        case "created":
          table.displayTable = chalk.green(table.table);
          table.displayBytes = chalk.green(table.bytesAfter);
          table.displayRows = chalk.green(table.rowsAfter);
          break;
        case "deleted":
          table.displayTable = chalk.red(table.table);
          table.displayBytes = chalk.red(table.bytesBefore);
          table.displayRows = chalk.red(table.rowsBefore);
          break;
        default:
          table.displayTable = table.table;
          table.displayBytes = valueOrDiff(table.bytesBefore, table.bytesAfter);
          table.displayRows = valueOrDiff(table.rowsBefore, table.rowsAfter);
          table.status =
            table.bytesBefore === table.bytesAfter &&
            table.rowsBefore === table.rowsAfter
              ? "similar"
              : "changed";
      }
      return table;
    })
    .value();
};

module.exports.diffSchemas = diffSchemas;
