package model;

import parser.CommandType;

public class DropTableCommand extends ParsedCommand {

    private final String tableName;

    public DropTableCommand(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public CommandType getType() {
        return CommandType.DROP;
    }

    public String getTableName() {
        return tableName;
    }

}