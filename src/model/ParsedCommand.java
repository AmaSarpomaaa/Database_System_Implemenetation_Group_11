package model;

//import parser.Condition;
//import java.util.List;

public class ParsedCommand {

    private String tableName;

    public ParsedCommand(String tableName) {
        this.tableName = tableName;
    }

    String tableName() {
        return tableName;
    }
}