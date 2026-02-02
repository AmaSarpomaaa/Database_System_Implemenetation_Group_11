package dml;

import model.ParsedCommand;
import model.Result;
import util.DBException;

public interface DMLProcessor {

    Result insert(ParsedCommand cmd) throws DBException;

    Result select(ParsedCommand cmd) throws DBException;

    Result delete(ParsedCommand cmd) throws DBException;

    Result update(ParsedCommand cmd) throws DBException;
}
