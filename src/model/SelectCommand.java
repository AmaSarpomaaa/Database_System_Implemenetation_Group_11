package model;

import parser.CommandType;
import parser.IWhereTree;
import util.DBException;

/**
 * A class to represent a ParsedCommand for a Select statement.
 */
public class SelectCommand extends ParsedCommand {

    protected String[] tableNames;

    /*
     * array of 2 element string arrays, made up of pairs: [tableName, attributeName]<br>
     * if the table isn't specified, the tableName will be null<br>
     * if the command is a select * command (selecting all attributes), attributeNames will be null
     */
    protected String[][] attributeNames;

    protected IWhereTree whereTree;

    /*
     * A pair of strings representing the attribute to orderby in the form:
     * [tableName, attributeName]<br>
     * If the table isn't specified, the tableName will be null<br>
     * If there is no orderby clause, orderby will be null
     */
    protected String[] orderby;

    /**
     * Creates a SelectCommand of the form SELECT * FROM {tableNames} with no
     * WHERE or ORDERBY clause.
     * @param tableNames the names of the tables that are selected by the command
     */
    public SelectCommand(String[] tableNames) {
        this.tableNames = tableNames;
        this.attributeNames = null;
        this.whereTree = null;
        this.orderby = null;
    }

    public SelectCommand(String[] tableNames, String[][] attributeNames,
                         IWhereTree whereTree, String[] orderby) {
        this.tableNames = tableNames;
        this.attributeNames = attributeNames;
        this.whereTree = whereTree;
        this.orderby = orderby;
    }

    @Override
    public CommandType getType() {
        return CommandType.SELECT;
    }

    public String[] getTableNames() {
        return tableNames;
    }

    /**
     * @return The attributes to be selected, represented as an array of
     * 2-element string arrays, made up of pairs of the form:
     * [tableName, attributeName]<br>
     * If a table isn't specified, the tableName will be null<br>
     * If the command is a select * command (selecting all attributes),
     * attributeNames will be null
     */
    public String[][] getAttributeNames() {
        return attributeNames;
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
            return false;
        }
        else {
            return whereTree.evaluate(scheme, record);
        }
    }

    /**
     * @return true if the command is a SELECT * command; false otherwise
     */
    public boolean isSelectStar() {
        return attributeNames == null;
    }

    /**
     * @return true if the command has an ORDERBY clause; false otherwise
     */
    public boolean hasWhere() {
        return whereTree == null;
    }

    /**
     * @return A pair of strings representing the attribute to orderby in the
     * form: [tableName, attributeName]<br>
     * If the table isn't specified, the tableName will be null;<br><br>
     * null if there is no orderby clause
     */
    public String[] getOrderby() {
        return orderby;
    }

    /**
     * @return true if the command has an ORDERBY clause; false otherwise
     */
    public boolean hasOrderby() {
        return orderby == null;
    }

}
