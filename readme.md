## sql-cli

Universal command-line interface for SQL databases.

### Installation

While this is a private repo, install it with:

```
npm install -g git+ssh://git@github.com/dgoguerra/sql-cli.git
```

### Usage

The supported commands are:

```
sql <command>

Commands:
  sql list <conn>             List tables                          [aliases: ls]
  sql show <table>            Show table structure
  sql diff <table1> <table2>  Diff two tables
  sql alias <action>          Manage saved connection aliases
  sql export <conn>           Export a connection's schema or data
  sql dump <conn> [name]      Dump the connection's schema
  sql load <conn> <dump>      Load a dump to the connection's schema
  sql shell <conn>            Run REPL shell                       [aliases: sh]

Options:
  --client, -c  Knex client adapter                                     [string]
  --version     Show version number                                    [boolean]
  -h, --help    Show help                                              [boolean]
```

Run `sql [command] --help` to see any command usage info.
