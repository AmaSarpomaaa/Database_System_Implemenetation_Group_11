package model;

import parser.Condition;
import java.util.List;

public interface ParsedCommand {

    CommandType type();

    String tableName();

    List<String> projections();   // empty = SELECT *

    Condition where();            // nullable

    List<List<Value>> rows();     // for INSERT
}
