const chalk = require("chalk");
const stringWidth = require("string-width");

const DEFAULT_WIDTH =
  process.env.SQL_LINE_WIDTH || process.stdout.columns || 80;

const DEFAULT_SEPARATOR = "...";

const linesSummary = (lines, { maxLines = 20 } = {}) => {
  const blockSize = Math.floor(maxLines / 2);

  if (lines.length <= 2 * blockSize + 1) {
    return lines;
  }

  const numHidden = lines.length - 2 * blockSize;

  return lines
    .splice(0, blockSize)
    .concat([`... (hiding ${numHidden} rows) ...`])
    .concat(lines.splice(-blockSize, blockSize));
};

const limitLine = (
  line,
  { width = DEFAULT_WIDTH, separator = DEFAULT_SEPARATOR } = {}
) => {
  let lineWidth = stringWidth(line);

  if (lineWidth <= width) {
    return line;
  }

  // Reset color when printing separator, in case any color code
  // is cut with substr() before its end.
  separator = chalk.reset(separator);

  const maxWidth = width - stringWidth(separator);

  // Since the line contains color escape codes, we dont know how
  // many characters to cut to limit it visually to the given width.
  // We only know how many characters are overflowing visually,
  // so just cut that amount and check again the line's visual width
  // until it doesnt overflow anymore.
  while (lineWidth > maxWidth) {
    const extraWidth = lineWidth - maxWidth;
    line = line.substr(0, line.length - extraWidth);

    // Make sure any unfinished trailing escape codes are removed.
    line = line.replace(/\[\d?\d?$/, "");

    lineWidth = stringWidth(line);
  }

  // Pad resulting line with spaces, so all lines separators are aligned
  const missingWidth = Math.max(0, lineWidth - maxWidth);

  return `${line}${" ".repeat(missingWidth)}${separator}`;
};

module.exports.limitLine = limitLine;

const summarize = (lines, { maxLines = 20 } = {}) => {
  return linesSummary(lines, { maxLines }).map((line) => limitLine(line));
};

module.exports.summarize = summarize;
