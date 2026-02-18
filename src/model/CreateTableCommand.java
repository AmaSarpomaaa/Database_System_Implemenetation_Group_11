package model;

import parser.CommandType;

public class CreateTableCommand extends ParsedCommand {

    Attribute[] attributes;

    public CreateTableCommand(String tableName, Attribute[] attributes) {
        super(CommandType.CREATE, tableName);
        this.attributes = attributes;
    }

}