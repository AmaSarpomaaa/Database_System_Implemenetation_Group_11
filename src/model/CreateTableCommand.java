package model;

import parser.CommandType;

public class CreateTableCommand extends ParsedCommand {

    Attribute[] attributes;

    public CreateTableCommand(String tableName, Attribute[] attributes) {
        super(tableName);
        this.attributes = attributes;
    }

    @Override
    public CommandType getType() {
        return CommandType.CREATE;
    }
}