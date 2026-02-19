package model;

import parser.CommandType;

/**
 * A class to represent a ParsedCommand for a Select statement.
 * Currently an abstract class because all Select commands in phase 1 will be
 * instances of SimpleSelectCommand. Will be made into a non-abstract class
 * with more functionality in phase 2.
 * All SelectCommands should be checked before assuming they are
 * SimpleSelectCommands, but SimpleSelectCommands can be processed as
 * SelectCommands without checking by treating them like SelectCommands
 * with one table in the tableNames array.
 */
public abstract class SelectCommand extends ParsedCommand {

    protected String[] tableNames;

    public SelectCommand(String[] tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public CommandType getType() {
        return CommandType.SELECT;
    }

    public String[] getTableNames() {
        return tableNames;
    }

}

/*
Needed Additions for Phase 2
 - List of Attributes
   - tableName
   - attributeName
 - where (Condition class)
 - orderBy (attribute name)
 */

