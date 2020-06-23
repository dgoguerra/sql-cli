const xlsx = require("xlsx");

class ExcelBuilder {
  constructor() {
    this.book = xlsx.utils.book_new();
  }

  addSheet(name, rows) {
    const header = Object.keys(rows && rows.length ? rows[0] : {});
    const rawRows = [header];

    rows.forEach((row) => {
      rawRows.push(header.map((key) => row[key]));
    });

    xlsx.utils.book_append_sheet(
      this.book,
      xlsx.utils.aoa_to_sheet(rawRows),
      name
    );
  }

  writeFile(filePath) {
    xlsx.writeFile(this.book, filePath);
  }
}

module.exports = ExcelBuilder;
