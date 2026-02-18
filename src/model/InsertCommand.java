package model;

import parser.CommandType;

import java.util.ArrayList;
import java.util.List;

public class InsertCommand extends ParsedCommand {

    /**
     * @return A list of rows of values.
     * <br>
     * Each row is represented as a list, containing some number of values.
     * Each row should be equivalent to one row in the table, but since the
     * parser doesn't check for the number and type of the values, rows may not
     * actually contain the correct type and amount of values to go into the
     * table.
     * <br>
     * Each value is represented as a pair of Objects. The first Object in the
     * pair is the Datatype that the second Object should be interpreted as.
     * <br>
     * Strings always use Char for the Datatype regardless of whether they are
     * meant to be interpreted as Chars or Varchars.
     * <br>
     * Null values use null for both the Datatype and the value in the pair.
     */
    private List<List<Object[]>> values;
    private final String tableName;

    public InsertCommand(String tableName) {
        this.tableName = tableName;
        this.values = new ArrayList<>();
        addRow();
    }

    /**
     * Adds one value  to the end of the last row in the list of values.
     * @param type The Datatype of the value to be added.
     * @param value The value to be added.
     */
    private void addValue(Datatype type, Object value) {
        values.get(values.size() - 1).add(new Object[] {type, value});
    }

    /**
     * Adds a null value to the end of the last row in the list of values.
     */
    public void addNull() {
        addValue(null, null);
    }

    /**
     * Adds one integer to the end of the last row in the list of values.
     * @param value the integer to be added.
     */
    public void addInteger(int value) {
        addValue(Datatype.INTEGER, value);
    }

    /**
     * Adds one double to the end of the last row in the list of values.
     * @param value the double to be added.
     */
    public void addDouble(double value) {
        addValue(Datatype.DOUBLE, value);
    }

    /**
     * Adds one boolean to the end of the last row in the list of values.
     * @param value the boolean value to be added.
     */
    public void addBoolean(boolean value) {
        addValue(Datatype.BOOLEAN, value);
    }

    /**
     * Adds one string to the end of the last row in the list of values.
     * Strings always use Char for the Datatype regardless of whether they are
     * meant to be interpreted as Chars or Varchars.
     * @param value the string to be added.
     */
    public void addString(double value) {
        addValue(Datatype.CHAR, value);
    }

    /**
     * Adds an empty row to the list of values.
     */
    public void addRow() {
        values.add(new ArrayList<>());
    }

    @Override
    public CommandType getType() {
        return CommandType.INSERT;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * @return A list of rows of values.
     * <br>
     * Each row is represented as a list, containing some number of values.
     * Each row should be equivalent to one row in the table, but since the
     * parser doesn't check for the number and type of the values, rows may not
     * actually contain the correct type and amount of values to go into the
     * table.
     * <br>
     * Each value is represented as a pair of Objects. The first Object in the
     * pair is the Datatype that the second Object should be interpreted as.
     * <br>
     * Strings always use Char for the Datatype regardless of whether they are
     * meant to be interpreted as Chars or Varchars.
     * <br>
     * Null values use null for both the Datatype and the value in the pair.
     */
    public List<List<Object[]>> getValues() {
        return values;
    }

}