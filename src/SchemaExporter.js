const xlsx = require("xlsx");

class SchemaExporter {
  constructor(lib) {
    this.lib = lib;
  }

  async writeFile(filePath) {
    const book = xlsx.utils.book_new();

    for (const table of await this.lib.listTables()) {
      xlsx.utils.book_append_sheet(
        book,
        await this.buildTableWorksheet(table),
        table.table
      );
    }

    xlsx.writeFile(book, filePath);
  }

  async buildTableWorksheet(table) {
    const rows = await this.lib.getTableSchema(table.table);

    const rawRows = [["Column", "Type", "Nullable"]];
    Object.keys(rows).forEach((key) => {
      const val = rows[key];
      rawRows.push([
        key,
        val.maxLength ? `${val.type}(${val.maxLength})` : val.type,
        val.nullable,
      ]);
    });

    return xlsx.utils.aoa_to_sheet(rawRows);
  }
}

module.exports = SchemaExporter;
