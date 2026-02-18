package model;

import parser.CommandType;

public abstract class ParsedCommand {

    protected String tableName;
    protected CommandType type;

    public ParsedCommand(CommandType type, String tableName) {
        this.type = type;
        this.tableName = tableName;
    }

    public CommandType getType() {
        return type;
    }

    public String getTableName() {
        return tableName;
    }

}