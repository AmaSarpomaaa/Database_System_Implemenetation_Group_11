package model;

import parser.CommandType;

/**
 * An abstract class to represent a parsed command.
 * Each class inheriting from ParsedCommand should correspond to one type of
 * command from the CommandType enum and should return that CommandType from
 * the getType() method, so that that method always returns the type of
 * command represented by the class.
 */
public abstract class ParsedCommand {

    protected String tableName;

    public ParsedCommand(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the CommandType represented by this object's class
     */
    public abstract CommandType getType();

    public String getTableName() {
        return tableName;
    }

}