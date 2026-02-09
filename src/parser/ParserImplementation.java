package parser;

import model.ParsedCommand;
import util.ParseException;
import java.util.Scanner;

public class ParserImplementation implements Parser
{

    /**
     * Parses an input string into a ParsedCommand that can be used by the
     * DDL or DML Processor.
     *
     * @param input A single command of input, represented as a string.
     * @return A ParsedCommand describing the command represented by the input
     * @throws ParseException If the input does not contain a command or
     * contains an improperly formatted command.
     */
    @Override
    public ParsedCommand parse(String input) throws ParseException {

        Scanner inputScanner = new Scanner(input);

        if (inputScanner.hasNext()) {
            String commandType = inputScanner.next();

            return switch (commandType) {
                case "CREATE" -> parseCreate(input);
                case "SELECT" -> parseSelect(input);
                case "INSERT" -> parseInsert(input);
                case "DROP" -> parseDrop(input);
                case "ALTER" -> parseAlter(input);
                default -> throw new ParseException("Error: invalid command");
            };
        }
        else
        {
            throw new ParseException("Error: empty input");
        }
    }

    private ParsedCommand parseCreate(String input) throws ParseException
    {
        throw new UnsupportedOperationException("parseCreate has not been implemented yet.");
    }

    private ParsedCommand parseSelect(String input) throws ParseException
    {
        throw new UnsupportedOperationException("parseSelect has not been implemented yet.");
    }

    private ParsedCommand parseInsert(String input) throws ParseException
    {
        throw new UnsupportedOperationException("parseInsert has not been implemented yet.");
    }

    private ParsedCommand parseDrop(String input) throws ParseException
    {
        throw new UnsupportedOperationException("parseDrop has not been implemented yet.");
    }

    private ParsedCommand parseAlter(String input) throws ParseException
    {
        throw new UnsupportedOperationException("parseAlter has not been implemented yet.");
    }

}
