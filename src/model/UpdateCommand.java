package model;

import parser.CommandType;
import java.util.List;

public class UpdateCommand extends ParsedCommand {

    private final String tableName;
    private final String attribute;
    private final Object value;
    private final List<Condition> conditions; // null means update all rows

    public UpdateCommand(String tableName, String attribute, Object value, List<Condition> conditions) {
        this.tableName = tableName;
        this.attribute = attribute;
        this.value = value;
        this.conditions = conditions;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttribute() {
        return attribute;
    }

    public Object getValue() {
        return value;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public CommandType getType() {
        return CommandType.UPDATE;
    }
}