package model;

import buffer.BufferManager;
import catalog.Catalog;
import parser.CommandType;
import parser.IWhereTree;
import storage.StorageManager;
import util.DBException;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Executes the FROM clause: looks up all tables by name and
     * iteratively builds a Cartesian product if more than one table.
     * @param catalog the catalog used to look up tables by name
     * @param storage the storage manager used to construct temp tables
     * @param buffer the buffer manager used to construct temp tables
     * @return a single Table representing the FROM result
     * @throws DBException if any table doesn't exist or scan fails
     */
    public Table from(Catalog catalog, StorageManager storage, BufferManager buffer) throws DBException {
        if (tableNames.length == 1) {
            return catalog.getTable(tableNames[0]);
        }

        Table result = catalog.getTable(tableNames[0]);

        for (int i = 1; i < tableNames.length; i++) {
            Table right = catalog.getTable(tableNames[i]);
            result = cartesianProduct(result, right, catalog, storage, buffer);
        }

        return result;
    }

    /**
     * Creates a new temporary table that is the Cartesian product of left and right.
     */
    private Table cartesianProduct(Table left, Table right, Catalog catalog,
                                   StorageManager storage, BufferManager buffer) throws DBException {
        List<Attribute> mergedAttrs = new ArrayList<>();
        for (Attribute a : left.schema().getAttributes()) {
            mergedAttrs.add(new Attribute(
                    left.name() + "." + a.getName(),
                    false, false, a.getType(), a.getDataLength()
            ));
        }
        for (Attribute a : right.schema().getAttributes()) {
            mergedAttrs.add(new Attribute(
                    right.name() + "." + a.getName(),
                    false, false, a.getType(), a.getDataLength()
            ));
        }

        // Mark first attribute as PK to satisfy schema constraint
        Attribute first = mergedAttrs.get(0);
        mergedAttrs.set(0, new Attribute(first.getName(), false, true, first.getType(), first.getDataLength()));

        String tempName = "__temp_" + left.name() + "_" + right.name();
        TableSchema temp = new TableSchema(tempName, new Schema(mergedAttrs), storage, buffer);
        catalog.addTable(temp);

        for (Record leftRec : left.scan()) {
            for (Record rightRec : right.scan()) {
                Record combined = new Record();
                for (Value v : leftRec.getAttributes()) combined.addAttribute(v);
                for (Value v : rightRec.getAttributes()) combined.addAttribute(v);
                temp.insert(combined);
            }
        }

        return temp;
    }

    /**
     * Executes the ORDERBY clause: sorts the records in the given table
     * by the specified attribute ascending, returns a new temp table.
     * If orderby is null, returns the table unchanged.
     * @param table the table to sort
     * @param storage the storage manager used to construct the temp table
     * @param buffer the buffer manager used to construct the temp table
     * @return a Table with records sorted by the orderby attribute
     * @throws DBException if the orderby attribute does not exist
     */
    public Table orderBy(Table table, StorageManager storage, BufferManager buffer) throws DBException {
        if (orderby == null) return table;

        String attrName;
        if (orderby[0] != null) {
            attrName = orderby[0] + "." + orderby[1];
        } else {
            attrName = orderby[1];
        }

        int attrIndex = table.schema().getAttributeIndex(attrName);
        if (attrIndex == -1) {
            throw new DBException("ORDERBY attribute does not exist: " + attrName);
        }

        List<Record> records = new ArrayList<>(table.scan());
        records.sort((r1, r2) -> {
            Object v1 = r1.getValue(attrIndex).getRaw();
            Object v2 = r2.getValue(attrIndex).getRaw();
            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return -1;
            if (v2 == null) return 1;
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                return ((Comparable<Object>) v1).compareTo(v2);
            }
            return v1.toString().compareTo(v2.toString());
        });

        String tempName = "__temp_orderby_" + table.name();
        TableSchema sorted = new TableSchema(tempName, table.schema(), storage, buffer);
        for (Record r : records) sorted.insert(r);
        return sorted;
    }
}

