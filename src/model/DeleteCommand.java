package model;

import parser.CommandType;
import parser.IWhereTree;
import util.DBException;

import java.util.List;

public class DeleteCommand extends ParsedCommand {

    private final String tableName;
    protected IWhereTree whereTree; // null means delete all rows

    public DeleteCommand(String tableName, IWhereTree whereTree) {
        this.tableName = tableName;
        this.whereTree = whereTree;
    }

    public String getTableName() {
        return tableName;
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
    public IWhereTree getWhereTree() {
        return whereTree;
    }

    @Override
    public CommandType getType() {
        return CommandType.DELETE;
    }
}