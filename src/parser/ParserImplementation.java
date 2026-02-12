package parser;

import model.ParsedCommand;
import util.ParseException;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.regex.Pattern;

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

        input = input.replace('\n', ' ');

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

    /**
     * Determines if a string is composed of only alphanumeric characters
     *
     * @param str The string to be checked.
     * @return true if the string contains only alphanumeric characters;
     * false otherwise
     */
    private boolean isAlphanumeric(String str)
    {

        for (int i = 0; i < str.length(); i++)
        {
            if (!Character.isLetterOrDigit(str.charAt(i)))
            {
                return false;
            }
        }

        return true;

    }

    private ParsedCommand parseCreate(String input) throws ParseException
    {
        Scanner inputScanner = new Scanner(input);

        //Check for "CREATE TABLE"
        if (!inputScanner.hasNext() || !inputScanner.next().equals("CREATE")) {
            if (!inputScanner.hasNext() || !inputScanner.next().equals("TABLE")) {
                throw new ParseException("Error: invalid command");
            }
        }

        //check and get " <tableName>"
        String tableName;
        if (inputScanner.hasNext()) {
            tableName = inputScanner.next();
        }
        else {
            throw new ParseException("Error: table name missing");
        }

        if (!isAlphanumeric(tableName)) {
            throw new ParseException("Error: table name composed of non-alphanumeric characters");
        }

        //check for " ("
        if (!inputScanner.hasNext() || !inputScanner.next().equals("(")) {
            throw new ParseException("Error: table attributes missing");
        }

        //parse attributes
        boolean done = false;
        inputScanner.useDelimiter(",");
        while (!done)
        {

            String attribute = inputScanner.next().trim();
            String[] attributeSplit = attribute.split(" +");

            if (attributeSplit.length < 2)
            {
                throw new ParseException("Error: attribute missing name or type");
            }

            //attribute name
            String attributeName = attributeSplit[0].toLowerCase();
            if (!isAlphanumeric(attributeName)) {
                throw new ParseException("Error: attribute name composed of non-alphanumeric characters");
            }

            //attribute type
            String attributeTypeStr = attributeSplit[1];
            /*
             * I don't think there is an attribute type object right now.
             * If there is later, attributeType should be switched to use that.
             * For now this is an array.
             *   One element for INTEGER, DOUBLE, and BOOLEAN.
             *   Two elements for CHAR and VARCHAR.
             *   The first element is the name of the type.
             *   The second element is the length for CHAR and VARCHAR.
             */
            String[] attributeType;

            if (attributeTypeStr.equals("INTEGER"))
            {
                attributeType = new String[] {"INTEGER"};
            }
            else if (attributeTypeStr.equals("DOUBLE"))
            {
                attributeType = new String[] {"DOUBLE"};
            }
            else if (attributeTypeStr.equals("BOOLEAN"))
            {
                attributeType = new String[] {"BOOLEAN"};
            }
            else if (Pattern.matches("CHAR\\(\\d+\\)", attributeTypeStr))   //matches CHAR(<a number>))
            {

                int start = attributeTypeStr.indexOf('(') + 1;
                int end = attributeTypeStr.indexOf(')');

                String n = attributeTypeStr.substring(start, end);
                attributeType = new String[] {"CHAR", n};

            }
            else if (Pattern.matches("VARCHAR\\(\\d+\\)", attributeTypeStr))   //matches VARCHAR(<a number>))
            {

                int start = attributeTypeStr.indexOf('(') + 1;
                int end = attributeTypeStr.indexOf(')');

                String n = attributeTypeStr.substring(start, end);
                attributeType = new String[] {"VARCHAR", n};
            }
            else
            {
                throw new ParseException("Error: invalid attribute type");
            }

            //attribute conditions
            if (attributeSplit.length > 2)
            {

            }

        }

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
