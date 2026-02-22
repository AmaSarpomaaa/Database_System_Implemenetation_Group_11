package parser;

import model.Attribute;
import model.Datatype;
import model.ParsedCommand;
import model.CreateTableCommand;
import model.SimpleSelectCommand;
import model.InsertCommand;
import model.DropTableCommand;
import model.AlterTableAddCommand;
import model.AlterTableDropCommand;
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
                default -> throw new ParseException("Invalid command");
            };
        }
        else
        {
            throw new ParseException("Empty input");
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

        //Check for "CREATE TABLE <tableName> (<something>);
        Pattern pattern = Pattern.compile("CREATE TABLE (\\w+) *\\((.*)\\);");
        Matcher matcher = pattern.matcher(input);

        //extract tableName
        String tableName;
        String attributesString;

        if (matcher.matches()) {

            tableName = matcher.group(1).toLowerCase();
            attributesString = matcher.group(2);

            if (!isAlphanumeric(tableName)) {
                throw new ParseException("Table name \"" + tableName + "\" composed of non-alphanumeric characters");
            }

        }
        else {
            throw new ParseException("Invalid command syntax.");
        }

        //extract attributes
        List<Attribute> attributes = new ArrayList<>();

        String[] attributesSplit = attributesString.trim().split(",");

        for (String attributeString : attributesSplit) {

            String[] attributeSplit = attributeString.trim().split(" ");

            String attributeName;
            Datatype attributeType;
            int attributeTypeLength;     //-1 for anything other than CHAR and VARCHAR
            boolean isPrimaryKey;
            boolean notNull;

            if (attributeSplit.length < 2) {
                throw new ParseException("Attribute missing name or type in \"" + attributeString + "\"");
            }
            else if (attributeSplit.length < 5) {

                //extract attribute name
                attributeName = attributeSplit[0].toLowerCase();

                if (!isAlphanumeric(attributeName)) {
                    throw new ParseException("Attribute name \"" + attributeName + "\" composed of non-alphanumeric characters");
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

                    int n = Integer.parseInt(attributeTypeStr.substring(start, end));
                    attributeType = Datatype.CHAR;
                    attributeTypeLength = n;

                }
                else if (Pattern.matches("VARCHAR\\(\\d+\\)", attributeTypeStr)) {   //matches VARCHAR(<a number>))

                    int start = attributeTypeStr.indexOf('(') + 1;
                    int end = attributeTypeStr.indexOf(')');

                    int n = Integer.parseInt(attributeTypeStr.substring(start, end));
                    attributeType = Datatype.VARCHAR;
                    attributeTypeLength = n;
                }
                else {
                    throw new ParseException("Attribute Type \"" + attributeTypeStr + "\" was not a valid type.");
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
                        throw new ParseException("Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
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
                        throw new ParseException("Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
                    }

                    if (attributeSplit[3].equals("PRIMARYKEY")) {
                        isPrimaryKey = true;
                    }
                    else if (attributeSplit[3].equals("NOTNULL")) {
                        notNull = true;
                    }
                    else {
                        throw new ParseException("Attribute constraint \"" + attributeSplit[2] + "\" was not a valid constraint.");
                    }

                    if (attributeSplit[2].equals(attributeSplit[3])) {
                        throw new ParseException("Duplicate attribute constraints in \"" + attributeString + "\".");
                    }

                }

            }
            else
            {
                throw new ParseException("Attribute \"" + attributeString + "\" included more than 4 pieces of information");
            }

            Attribute attribute = new Attribute(attributeName, notNull, isPrimaryKey, attributeType, attributeTypeLength);
            attributes.add(attribute);

        }

        //turn the attribute list into an array
        Object[] attributeArrayAsObject = attributes.toArray();
        Attribute[] attributeArray = new Attribute[attributeArrayAsObject.length];

        for (int i = 0; i < attributeArrayAsObject.length; i++) {
            attributeArray[i] = (Attribute) attributeArrayAsObject[i];
        }

        return new CreateTableCommand(tableName, attributeArray);

    }

    //Currently only does "SELECT * FROM <tableName>;"
    private ParsedCommand parseSelect(String input) throws ParseException
    {
        //Check for "SELECT * FROM <tableName>;"
        Pattern pattern = Pattern.compile("SELECT \\* FROM (\\w+);");
        Matcher matcher = pattern.matcher(input);
        //CREATE TABLE t9 (a1 INTEGER PRIMARYKEY); INSERT t9 VALUES(1); SELECT * FROM t9;

        //extract tableName
        String tableName;

        if (matcher.matches()) {

            tableName = matcher.group(1).toLowerCase();

            if (!isAlphanumeric(tableName)) {
                throw new ParseException("Table name \"" + tableName + "\" composed of non-alphanumeric characters");
            }

        }
        else {
            throw new ParseException("Invalid command syntax.");
        }

        return new SimpleSelectCommand(tableName);
    }

    private ParsedCommand parseInsert(String input) throws ParseException
{

    //Check for "INSERT <tableName> VALUES(<something>);"
    Pattern pattern = Pattern.compile("INSERT (\\w+) VALUES *\\((.*)\\);");
    Matcher matcher = pattern.matcher(input);

    //extract tableName
    String tableName;
    String valuesString;

    if (matcher.matches()) {

        tableName = matcher.group(1).toLowerCase();
        valuesString = matcher.group(2);

        if (!isAlphanumeric(tableName)) {
            throw new ParseException("Table name \"" + tableName + "\" composed of non-alphanumeric characters");
        }

    }
    else {
        throw new ParseException("Invalid command syntax.");
    }

    //extract values
    InsertCommand command = new InsertCommand(tableName);

    //a row possibly followed by a comma, the capturing group is a row
    Matcher rowMatcher = Pattern.compile("((?:[^,\"]*(?:\"[^\"]*\")*)*),?").matcher(valuesString);

    while (rowMatcher.find()) {

        String row = rowMatcher.group(1).trim();

        if (!row.isEmpty()) {

            //tokenize by quoted strings or non-space sequences
            Matcher tokenMatcher = Pattern.compile("\"[^\"]*\"|[^ ]+").matcher(row);

            while (tokenMatcher.find()) {

                String value = tokenMatcher.group();


                if (!value.isEmpty()) {

                    //integer
                    try {
                        command.addInteger(Integer.parseInt(value));
                        continue;
                    }
                    catch (NumberFormatException ignored) {}

                    //double
                    try {
                        command.addDouble(Double.parseDouble(value));
                        continue;
                    }
                    catch (NumberFormatException ignored) {}

                    //boolean
                    if (value.equals("True")) {
                        command.addBoolean(true);
                        continue;
                    }
                    else if (value.equals("False")) {
                        command.addBoolean(false);
                        continue;
                    }

                    //null
                    if (value.equals("NULL")) {
                        command.addNull();
                        continue;
                    }

                    //string
                    Matcher charMatcher = Pattern.compile("\"([^\"]*)\"").matcher(value);

                    if (charMatcher.matches()) {
                        command.addString(charMatcher.group(1));
                        continue;
                    }

                    //invalid data
                    throw new ParseException("\"" + value + "\" was not of a valid data type.");

                }

            }

        }

        command.addRow();

    }

    return command;

}

    private ParsedCommand parseDrop(String input) throws ParseException
    {
        //Check for "DROP TABLE <tableName>;"
        Matcher matcher = Pattern.compile("DROP TABLE (\\w+);").matcher(input);

        //extract tableName
        String tableName;

        if (matcher.matches()) {

            tableName = matcher.group(1).toLowerCase();
            if (!isAlphanumeric(tableName)) {
                throw new ParseException("Table name \"" + tableName + "\" composed of non-alphanumeric characters");
            }

        }
        else {
            throw new ParseException("Invalid command syntax.");
        }

        return new DropTableCommand(tableName);
    }

    private ParsedCommand parseAlter(String input) throws ParseException
    {

        //Check for "ALTER TABLE <tableName>"
        Matcher matcher = Pattern.compile("ALTER TABLE (\\w+) (.*);").matcher(input);

        //extract tableName
        String tableName;
        String remainingText;

        if (matcher.matches()) {

            tableName = matcher.group(1).toLowerCase();
            remainingText = matcher.group(2);

            if (!isAlphanumeric(tableName)) {
                throw new ParseException("Table name \"" + tableName + "\" composed of non-alphanumeric characters");
            }

        }
        else {
            throw new ParseException("Invalid command syntax.");
        }

        //Check for ADD or DROP
        Matcher addMatcher = Pattern.compile("ADD (.*)").matcher(remainingText);
        Matcher dropMatcher = Pattern.compile("DROP (.*)").matcher(remainingText);

        if (addMatcher.matches()) {
            remainingText = addMatcher.group(1);
            return(parseAlterAdd(tableName, remainingText));
        }
        else if (dropMatcher.matches()) {
            remainingText = dropMatcher.group(1);
            return(parseAlterDrop(tableName, remainingText));
        }
        else {
            throw new ParseException("Invalid command syntax.");
        }

    }

    private ParsedCommand parseAlterAdd(String tableName, String remainingText) throws ParseException {

        //a value possibly followed by a space, the capturing group is a value
        Matcher matcher = Pattern.compile("(\\w+) (INTEGER|DOUBLE|BOOLEAN|CHAR\\((\\d+)\\)|VARCHAR\\((\\d+)\\))((?: NOTNULL)?)((?: DEFAULT (.+))?)").matcher(remainingText);

        //extract attributeName
        String attributeName;
        String attributeTypeStr;

        if (matcher.matches()) {
            attributeName = matcher.group(1);
            attributeTypeStr = matcher.group(2);
        }
        else {
            throw new ParseException("Invalid command format.");
        }

        //attempt to extract DEFAULT value
        boolean hasDefault = false;
        String defaultStr = "";
        Object defaultValue = null;

        if (!matcher.group(6).isEmpty())
        {
            hasDefault = true;
            defaultStr = matcher.group(7);

            if (defaultStr.equals("NULL")) {
                hasDefault = false;
            }
        }

        //determine attribute type
        Datatype attributeType;
        int attributeTypeLength;

        if (attributeTypeStr.equals("INTEGER")) {
            attributeType = Datatype.INTEGER;
            attributeTypeLength = -1;

            //extract DEFAULT
            if (hasDefault)
            {
                try {
                    defaultValue = Integer.parseInt(defaultStr);
                }
                catch (NumberFormatException e) {
                    throw new ParseException("Default Value " + defaultStr + " was not an INTEGER.", e);
                }
            }
        }
        else if (attributeTypeStr.equals("DOUBLE")) {
            attributeType = Datatype.DOUBLE;
            attributeTypeLength = -1;

            //extract DEFAULT
            if (hasDefault)
            {
                try {
                    defaultValue = Double.parseDouble(defaultStr);
                }
                catch (NumberFormatException e) {
                    throw new ParseException("Default Value " + defaultStr + " was not a DOUBLE.", e);
                }
            }
        }
        else if (attributeTypeStr.equals("BOOLEAN")) {
            attributeType = Datatype.BOOLEAN;
            attributeTypeLength = -1;

            //extract DEFAULT
            if (hasDefault)
            {
                try {
                    defaultValue = Boolean.parseBoolean(defaultStr);
                }
                catch (NumberFormatException e) {
                    throw new ParseException("Default Value " + defaultStr + " was not a BOOLEAN.", e);
                }
            }
        }
        else if (Pattern.matches("CHAR\\(\\d+\\)", attributeTypeStr)) {   //matches CHAR(<a number>))
            attributeType = Datatype.CHAR;
            attributeTypeLength = Integer.parseInt(matcher.group(3));

            //extract DEFAULT
            if (hasDefault)
            {
                Matcher charMatcher = Pattern.compile("\"((:?[^\"])*)\"").matcher(defaultStr);
                String defaultValueStr;

                if (charMatcher.matches()) {
                    defaultValueStr = charMatcher.group(1);
                }
                else {
                    System.out.println("Default: " + defaultStr);
                    throw new ParseException("Default Value " + defaultStr + " was not a CHAR.");
                }

                if (defaultValueStr.length() != attributeTypeLength) {
                    throw new ParseException("Default Value " + defaultStr + " was not the right length (" + attributeTypeLength + ").");
                }

                defaultValue = defaultValueStr;
            }
        }
        else if (Pattern.matches("VARCHAR\\(\\d+\\)", attributeTypeStr)) {   //matches VARCHAR(<a number>))
            attributeType = Datatype.VARCHAR;
            attributeTypeLength = Integer.parseInt(matcher.group(4));

            //extract DEFAULT
            if (hasDefault)
            {
                Matcher charMatcher = Pattern.compile("\"((:?[^\"])*)\"").matcher(defaultStr);
                String defaultValueStr;

                if (charMatcher.matches()) {
                    defaultValueStr = charMatcher.group(1);
                }
                else {
                    throw new ParseException("Default Value " + defaultStr + " was not a VARCHAR.");
                }

                if (defaultValueStr.length() > attributeTypeLength) {
                    throw new ParseException("Default Value " + defaultStr + " was longer than the maximum length of " + attributeTypeLength + ".");
                }

                defaultValue = defaultValueStr;
            }
        }
        else {
            //It shouldn't actually be possible to get here
            throw new ParseException("Attribute Type \"" + attributeTypeStr + "\" was not a valid type.");
        }

        //extract NOTNULL
        boolean notNull = !matcher.group(5).isEmpty();

        if (notNull && !hasDefault) {
            throw new ParseException("Not null requires a default value when altering a table");
        }

        Attribute attribute = new Attribute(attributeName, notNull, false, attributeType, attributeTypeLength);

        if (!hasDefault) {
            return new AlterTableAddCommand(tableName, attribute);
        }
        else {
            return new AlterTableAddCommand(tableName, attribute, defaultValue);
        }

    }

    private ParsedCommand parseAlterDrop(String tableName, String remainingText) throws ParseException {

        String attributeName;
        attributeName = remainingText;

        if (!isAlphanumeric(attributeName)) {
            throw new ParseException("Attribute name \"" + attributeName + "\" composed of non-alphanumeric characters");
        }

        return new AlterTableDropCommand(tableName, attributeName);

    }

}
