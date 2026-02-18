package model;

import parser.CommandType;

public class CreateTableCommand extends ParsedCommand {

    private final Attribute[] attributes;
    private final String tableName;

    public CreateTableCommand(String tableName, Attribute[] attributes) {
        this.tableName = tableName;
        this.attributes = attributes;
    }

    @Override
    public CommandType getType() {
        return CommandType.CREATE;
    }

    public String getTableName() {
        return tableName;
    }

    public Attribute[] getAttributes() {
        return attributes;
    }

}