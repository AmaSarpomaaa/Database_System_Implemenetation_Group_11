package model;

import parser.CommandType;

public class CreateTableCommand extends ParsedCommand {

    Attribute[] attributes;
    String tableName;

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

}