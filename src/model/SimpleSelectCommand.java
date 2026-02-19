package model;

import parser.CommandType;

/**
 * A simple version of a ParsedCommand for a Select * statement in phase 1.
 * Can only encode commands of the form "SELECT * FROM {tableName}".
 * A class to represent a ParsedCommand for a Select statement.
 * All SelectCommands should be checked before assuming they are
 * SimpleSelectCommands, but SimpleSelectCommands can be processed as
 * SelectCommands without checking by treating them like SelectCommands
 * with one table in the tableNames array.
 */
public class SimpleSelectCommand extends SelectCommand {

    public SimpleSelectCommand(String tableName) {
        super(new String[] {tableName});
    }

    public String getTableName() {
        return tableNames[0];
    }

}
