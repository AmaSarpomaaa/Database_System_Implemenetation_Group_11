package parser;

import model.ParsedCommand;
import util.ParseException;

public interface Parser {
    ParsedCommand parse(String input) throws ParseException;
}
