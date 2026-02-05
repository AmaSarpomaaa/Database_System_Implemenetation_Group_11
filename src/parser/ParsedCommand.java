package parser;

import java.util.List;
import java.util.concurrent.locks.Condition;

public class ParsedCommand {

    private CommandType type;
    private List<String> tableNames;
    private List<String> attributes;
    private List<Object> values;
    private Condition conditions;
    private String orderBy;

}