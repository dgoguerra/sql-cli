const fs = require("fs");
const _ = require("lodash");
const plist = require("plist");
const { stringifyConn } = require("./connUtils");

const TABLEPLUS_CONNECTIONS_PATH = `${process.env.HOME}/Library/Application Support/com.tinyapp.TablePlus/Data/Connections.plist`;

const SEQUELPRO_FAVORITES_PATH = `${process.env.HOME}/Library/Application Support/Sequel Pro/Data/Favorites.plist`;

const findTablePlusAliases = () => {
  let connections;

  try {
    connections = readPlistFile(TABLEPLUS_CONNECTIONS_PATH);
  } catch (err) {
    // File not found, assume TablePlus is not installed or has no config
    return;
  }

  return connections.map((c) => {
    const alias = _.snakeCase(c.ConnectionName).replace(/_/g, "-");
    const conn = stringifyConn({
      protocol: c.Driver.toLowerCase(),
      path: c.DatabasePath, // only for SQLite
      host: c.DatabaseHost,
      port: c.DatabasePort,
      user: c.DatabaseUser,
      database: c.DatabaseName,
      sshHost: c.isOverSSH && c.ServerAddress,
      sshPort: c.isOverSSH && c.ServerPort,
      sshUser: c.isOverSSH && c.ServerUser,
    });

    return {
      source: "tableplus",
      alias,
      conn,
      keychain: {
        service: "com.tableplus.TablePlus",
        account: `${c.ID}_database`,
      },
    };
  });
};

const findSequelProAliases = () => {
  let connections;
  try {
    const favorites = readPlistFile(SEQUELPRO_FAVORITES_PATH, {
      numbersAsString: true,
    });
    connections = favorites["Favorites Root"].Children;
  } catch (err) {
    // File not found, assume SequelPro is not installed or has no config
    return;
  }

  return connections.map((c) => {
    const alias = _.snakeCase(c.name).replace(/_/g, "-");
    const conn = stringifyConn({ protocol: "mysql", ...c });

    return {
      source: "sequelpro",
      alias,
      conn,
      keychain: {
        service: `Sequel Pro : ${c.name} (${c.id})`,
        account: `${c.user}@${c.host}/${c.database}`,
      },
    };
  });
};

const readPlistFile = (filePath, { numbersAsString = false } = {}) => {
  let content = fs.readFileSync(filePath).toString();

  // Fix: Sequel Pro uses very big numeric IDs, which JavaScript loads
  // as unsafe integers, losing precision. For example:
  //
  // > require('plist').parse('<plist><integer>7508107108915805319</integer></plist>')
  // 7508107108915805000
  // > Number.isSafeInteger(7508107108915805319)
  // false
  //
  // We work around this by converting all integers of the plist file to
  // string befofe parsing it.
  if (numbersAsString) {
    content = content.replace(/<integer>/g, "<string>");
    content = content.replace(/<\/integer>/g, "</string>");
  }

  return plist.parse(content);
};

const getExternalAliases = () => {
  return [].concat(findSequelProAliases()).concat(findTablePlusAliases());
};

module.exports = { getExternalAliases };
