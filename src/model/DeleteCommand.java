package model;

import parser.CommandType;
import java.util.List;

public class DeleteCommand extends ParsedCommand {

    private final String tableName;
    private final List<Condition> conditions; // null means delete all rows

    public DeleteCommand(String tableName, List<Condition> conditions) {
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public CommandType getType() {
        return CommandType.DELETE;
    }
}