package parser;

import model.Attribute;
import model.Datatype;
import model.ParsedCommand;
import model.CreateTableCommand;
import util.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
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

        //Check for "CREATE TABLE <tableName> (<something>);
        Pattern pattern = Pattern.compile("CREATE TABLE (\\w+) *\\((.*)\\);");
        Matcher matcher = pattern.matcher(input);

        //extract tableName
        String tableName;
        String attributesString;

        if (matcher.matches()) {
            tableName = matcher.group(1).toLowerCase();
            attributesString = matcher.group(2);
        }
        else {
            throw new ParseException("Error: Invalid command syntax.");
        }

        //extract attributes
        List<Attribute> attributes = new ArrayList<>();

        String[] attributesSplit = attributesString.split(",");

        for (String attributeString : attributesSplit) {

            String[] attributeSplit = attributeString.split(" ");

            String attributeName;
            Datatype attributeType;
            int attributeTypeLength;     //-1 for anything other than CHAR and VARCHAR
            boolean isPrimaryKey;
            boolean notNull;

            if (attributeSplit.length < 2) {
                throw new ParseException("Error: Attribute missing name or type in \"" + attributeString + "\"");
            }
            else if (attributeSplit.length < 5) {

                //extract attribute name
                attributeName = attributeSplit[0].toLowerCase();

                if (!isAlphanumeric(attributeName)) {
                    throw new ParseException("Error: Attribute name \"" + attributeName + "\" composed of non-alphanumeric characters");
                }

                //extract attribute type
                String attributeTypeStr = attributeSplit[1];

                if (attributeTypeStr.equals("INTEGER")) {
                    attributeType = Datatype.INTEGER;
                    attributeTypeLength = -1;
                }
                else if (attributeTypeStr.equals("DOUBLE")) {
                    attributeType = Datatype.DOUBLE;
                    attributeTypeLength = -1;
                }
                else if (attributeTypeStr.equals("BOOLEAN")) {
                    attributeType = Datatype.BOOLEAN;
                    attributeTypeLength = -1;
                }
                else if (Pattern.matches("CHAR\\(\\d+\\)", attributeTypeStr)) {   //matches CHAR(<a number>))

                    int start = attributeTypeStr.indexOf('(') + 1;
                    int end = attributeTypeStr.indexOf(')');

                    int n = Integer.getInteger(attributeTypeStr.substring(start, end));
                    attributeType = Datatype.CHAR;
                    attributeTypeLength = n;

                }
                else if (Pattern.matches("VARCHAR\\(\\d+\\)", attributeTypeStr)) {   //matches VARCHAR(<a number>))

                    int start = attributeTypeStr.indexOf('(') + 1;
                    int end = attributeTypeStr.indexOf(')');

                    int n = Integer.getInteger(attributeTypeStr.substring(start, end));
                    attributeType = Datatype.VARCHAR;
                    attributeTypeLength = n;
                }
                else {
                    throw new ParseException("Error: Attribute Type \"" + attributeTypeStr + "\" was not a valid type.");
                }

                //extract attribute constraints

                if (attributeSplit.length == 2) {
                    isPrimaryKey = false;
                    notNull = false;
                }
                else if (attributeSplit.length == 3) {
                    if (attributeSplit[2].equals("PRIMARYKEY")) {
                        isPrimaryKey = true;
                        notNull = false;
                    }
                    else if (attributeSplit[2].equals("NOTNULL")) {
                        notNull = true;
                        isPrimaryKey = false;
                    }
                    else {
                        throw new ParseException("Error: Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
                    }
                }
                else { // attributeSplit.length == 4
                    //both should be true by the end of the else statement, unless an error is thrown
                    //but the logic is complicated enough that they need to be assigned here to make
                    //intellij recognize that they're always initialized
                    isPrimaryKey = false;
                    notNull = false;

                    if (attributeSplit[2].equals("PRIMARYKEY")) {
                        isPrimaryKey = true;
                    }
                    else if (attributeSplit[2].equals("NOTNULL")) {
                        notNull = true;
                    }
                    else {
                        throw new ParseException("Error: Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
                    }

                    if (attributeSplit[3].equals("PRIMARYKEY")) {
                        isPrimaryKey = true;
                    }
                    else if (attributeSplit[3].equals("NOTNULL")) {
                        notNull = true;
                    }
                    else {
                        throw new ParseException("Error: Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
                    }

                    if (attributeSplit[2].equals(attributeSplit[3])) {
                        throw new ParseException("Error: Duplicate attribute constraints in \"" + attributeString + "\".");
                    }

                }

            }
            else
            {
                throw new ParseException("Error: Attribute \"" + attributeString + "\" included more than 4 pieces of information");
            }

            Attribute attribute = new Attribute(attributeName, notNull, isPrimaryKey, attributeType);
            attributes.add(attribute);

        }

        return new CreateTableCommand(tableName, attributes.toArray());

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
