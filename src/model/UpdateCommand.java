package model;

import parser.CommandType;
import parser.IWhereTree;
import util.DBException;

import java.util.List;

public class UpdateCommand extends ParsedCommand {

    private final String tableName;
    private final String attribute;
    private final Object value;
    protected IWhereTree whereTree; // null means update all rows

    public UpdateCommand(String tableName, String attribute, Object value, IWhereTree whereTree) {
        this.tableName = tableName;
        this.attribute = attribute;
        this.value = value;
        this.whereTree = whereTree;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttribute() {
        return attribute;
    }

    public Object getValue() {
        return value;
    }

    /**
     * Evaluates the root node of the WHERE tree against a specific Record
     * @param scheme schema of the table used to find index of attr names
     * @param record row of the data being checked
     * @return false if the command doesn't have a where clause;
     * true if the command has a where clause and the record passes conditions;
     * false otherwise
     * @throws DBException I guess if there is an invalid column name or type mismatch
     */
    public boolean where(Schema scheme, Record record) throws DBException {
        if (whereTree == null) {
            return true;
        }
        else {
            return whereTree.evaluate(scheme, record);
        }
    }

    /**
     * @return true if the command has a WHERE clause; false otherwise
     */
    public boolean hasWhere() {
        return whereTree == null;
    }

    @Override
    public CommandType getType() {
        return CommandType.UPDATE;
    }

    public IWhereTree getWhereTree() {
        return whereTree;
    }
}