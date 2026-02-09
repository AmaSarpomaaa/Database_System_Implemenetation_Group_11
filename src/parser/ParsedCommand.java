package parser;

import java.util.List;

public class ParsedCommand {

    private final CommandType type;
    private final List<String> tableNames;
    private final List<String> attributes;
    private final List<Object> values;
    private final String orderBy;

    public ParsedCommand(CommandType type, List<String> tableNames, List<String> attributes, List<Object> values, String orderBy)
    {
        this.type = type;
        this.tableNames = tableNames;
        this.attributes = attributes;
        this.values = values;
//        this.conditions = conditions;
        this.orderBy = orderBy;
    }

    public CommandType getType() {
        return type;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public List<Object> getValues() {
        return values;
    }

    public String getOrderBy() {
        return orderBy;
    }

}