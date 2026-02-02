package ddl;

import model.ParsedCommand;
import model.Result;
import util.DBException;

public interface DDLProcessor {

    Result createTable(ParsedCommand cmd) throws DBException;

    Result dropTable(ParsedCommand cmd) throws DBException;

    Result alterTableAdd(ParsedCommand cmd) throws DBException;

    Result alterTableDrop(ParsedCommand cmd) throws DBException;
}
