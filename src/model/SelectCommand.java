package model;

import parser.CommandType;
import catalog.FileCatalog;
import util.DBException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to represent a ParsedCommand for a Select statement.
 * Currently an abstract class because all Select commands in phase 1 will be
 * instances of SimpleSelectCommand. Will be made into a non-abstract class
 * with more functionality in phase 2.
 * All SelectCommands should be checked before assuming they are
 * SimpleSelectCommands, but SimpleSelectCommands can be processed as
 * SelectCommands without checking by treating them like SelectCommands
 * with one table in the tableNames array.
 */
public abstract class SelectCommand extends ParsedCommand {

    protected String[] tableNames;
    protected String orderByAttr;

    public SelectCommand(String[] tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public CommandType getType() {
        return CommandType.SELECT;
    }

    public String[] getTableNames() {
        return tableNames;
    }

    public void setOrderBy(String attr) {
        this.orderByAttr = attr;
    }

    public String getOrderByAttr() {
        return orderByAttr;
    }

    public Table from(FileCatalog catalog) throws DBException {
        if (tableNames.length == 1) {
            return catalog.getTable(tableNames[0]);
        }

        // Start with the first table
        Table result = catalog.getTable(tableNames[0]);

        // Join with each next table
        for (int i = 1; i < tableNames.length; i++) {
            Table right = catalog.getTable(tableNames[i]);
            result = cartesianProduct(result, right);
        }

        return result;
    }

    /**
     * Creates a new temporary in-memory table that is the Cartesian
     * product of left and right
     */
    private Table cartesianProduct(Table left, Table right) throws DBException {
        List<Attribute> mergedAttrs = new ArrayList<>();
        for (Attribute a : left.schema().getAttributes()) {
            mergedAttrs.add(new Attribute(left.name() + "." + a.getName(), false, false, a.getType(), a.getDataLength()));
        }
        for (Attribute a : right.schema().getAttributes()) {
            mergedAttrs.add(new Attribute(right.name() + "." + a.getName(), false, false, a.getType(), a.getDataLength()));
        }

        Schema mergedSchema = new Schema(mergedAttrs);
        TempTable temp = new TempTable(left.name() + "_" + right.name(), mergedSchema);

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

    public Table orderBy(Table table) throws DBException {
        if (orderByAttr == null) return table;

        // Find index of the attribute in the schema
        int attrIndex = table.schema().getAttributeIndex(orderByAttr);
        if (attrIndex == -1) {
            throw new DBException("ORDERBY attribute does not exist: " + orderByAttr);
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

        TempTable sorted = new TempTable(table.name(), table.schema());
        for (Record r : records) sorted.insert(r);
        return sorted;
    }
}

/*
Needed Additions for Phase 2
 - List of Attributes
   - tableName
   - attributeName
 - where (Condition class)
 - orderBy (attribute name)
 */

