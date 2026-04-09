package model;

import buffer.BufferManager;
import catalog.Catalog;
import parser.CommandType;
import parser.IWhereTree;
import storage.StorageManager;
import util.DBException;
import java.util.ArrayList;
import java.util.List;
import ddl.DDLParser;
import java.util.Comparator;

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
        String[] array = null;
        this.orderby = array;
    }

    public SelectCommand(String[] tableNames, String[][] attributeNames,
                         IWhereTree whereTree, String[] orderby) {
        this.tableNames = tableNames;
        this.attributeNames = attributeNames;
        this.whereTree = whereTree;
        String[] array = orderby;
        this.orderby = array;
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
            return true;
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
    public Table from(Catalog catalog, StorageManager storage, BufferManager buffer, DDLParser ddl) throws DBException {
        if (tableNames.length == 1) {
            return catalog.getTable(tableNames[0]);
        }

        Table result = catalog.getTable(tableNames[0]);

        for (int i = 1; i < tableNames.length; i++) {
            Table right = catalog.getTable(tableNames[i]);
            Table next = cartesianProduct(result, right, catalog, storage, buffer);

            if (i > 1) {
                ddl.dropTable(result.name());
            }

            result = next;
        }

        return result;
    }

    /**
     * Creates a new temporary table that is the Cartesian product of left and right.
     */
    private Table cartesianProduct(Table left, Table right, Catalog catalog,
                                   StorageManager storage, BufferManager buffer) throws DBException {
        List<Attribute> mergedAttrs = new ArrayList<>();

        // Synthetic integer PK
        mergedAttrs.add(new Attribute("__pk", false, true, Datatype.INTEGER, 4));

        for (Attribute a : left.schema().getAttributes()) {
            if (a.getName().equals("__pk") || a.getName().endsWith(".__pk")) continue;
            String qualifiedName = a.getName().contains(".") ? a.getName() : left.name() + "." + a.getName();
            mergedAttrs.add(new Attribute(qualifiedName, false, false, a.getType(), a.getDataLength()));
        }
        for (Attribute a : right.schema().getAttributes()) {
            if (a.getName().equals("__pk") || a.getName().endsWith(".__pk")) continue;
            String qualifiedName = a.getName().contains(".") ? a.getName() : right.name() + "." + a.getName();
            mergedAttrs.add(new Attribute(qualifiedName, false, false, a.getType(), a.getDataLength()));
        }

        String tempName = "__temp_" + left.name() + "_" + right.name();
        TableSchema temp = new TableSchema(tempName, new Schema(mergedAttrs), storage, buffer, true);
        catalog.addTable(temp);

        int pk = 0;
        for (int lpid : ((TableSchema) left).getPageIds()) {
            Page lPage = buffer.getPage(lpid);
            for (Record leftRec : lPage.getRecords()) {

                for (int rpid : ((TableSchema) right).getPageIds()) {
                    Page rPage = buffer.getPage(rpid);
                    for (Record rightRec : rPage.getRecords()) {

                        Record combined = new Record();
                        combined.addAttribute(new Value(pk++));

                        List<Attribute> leftAttrs = left.schema().getAttributes();
                        for (int i = 0; i < leftAttrs.size(); i++) {
                            String name = leftAttrs.get(i).getName();
                            if (name.equals("__pk") || name.endsWith(".__pk")) continue;
                            combined.addAttribute(leftRec.getAttributes().get(i));
                        }

                        List<Attribute> rightAttrs = right.schema().getAttributes();
                        for (int i = 0; i < rightAttrs.size(); i++) {
                            String name = rightAttrs.get(i).getName();
                            if (name.equals("__pk") || name.endsWith(".__pk")) continue;
                            combined.addAttribute(rightRec.getAttributes().get(i));
                        }

                        temp.insert(combined);
                    }
                }
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
    public Table orderBy(Table table, Catalog catalog, StorageManager storage, BufferManager buffer, DDLParser ddl) throws DBException {
        if (orderby == null) { return table; }

        String orderAttrName = orderby[orderby.length - 1];
        Schema schema = table.schema();
        List<Attribute> attrs = schema.getAttributes();

        int orderIndex = -1;
        for (int i = 0; i < attrs.size(); i++) {
            String attrName = attrs.get(i).getName();
            if (attrName.equals(orderAttrName) || attrName.endsWith("." + orderAttrName)) {
                orderIndex = i;
                break;
            }
        }
        if (orderIndex == -1) {
            throw new DBException("ORDERBY attribute not found: " + orderAttrName);
        }

        final int idx = orderIndex;
        Comparator<Record> cmp = Comparator.comparing(r -> (Comparable) r.getAttributes().get(idx).getRaw());

        if (!(table instanceof TableSchema ts)) {
            throw new DBException("Unsupported table type");
        }

        // Phase 1: sort each page individually, write out as sorted run tables
        List<TableSchema> runs = new ArrayList<>();
        for (int pid : ts.getPageIds()) {
            Page p = buffer.getPage(pid);
            List<Record> pageRecords = new ArrayList<>(p.getRecords()); // one page at a time
            pageRecords.sort(cmp);

            String runName = "__run_" + runs.size() + "_" + table.name();
            TableSchema run = new TableSchema(runName, schema, storage, buffer, true);
            catalog.addTable(run);
            for (Record r : pageRecords) {
                run.append(r);
            }
            runs.add(run);
        }

        // Phase 2: k-way merge — one record per run in memory at a time
        String tempName = "__orderby_" + table.name();
        TableSchema tempTable = new TableSchema(tempName, schema, storage, buffer, true);
        catalog.addTable(tempTable);

        // Track current position in each run
        List<List<Integer>> runPageIds = new ArrayList<>();
        List<Integer> pageIdx = new ArrayList<>();
        List<Integer> recordIdx = new ArrayList<>();
        List<Record> heads = new ArrayList<>(); // one record per run

        for (TableSchema run : runs) {
            runPageIds.add(run.getPageIds());
            pageIdx.add(0);
            recordIdx.add(0);
            heads.add(nextRecord(run.getPageIds(), 0, 0, buffer));
        }

        while (true) {
            // find run with smallest head
            int minRun = -1;
            for (int i = 0; i < heads.size(); i++) {
                if (heads.get(i) == null) continue;
                if (minRun == -1 || cmp.compare(heads.get(i), heads.get(minRun)) < 0) {
                    minRun = i;
                }
            }
            if (minRun == -1) break; // all exhausted

            tempTable.append(heads.get(minRun));

            // advance that run
            int rIdx = recordIdx.get(minRun);
            int pIdx = pageIdx.get(minRun);
            List<Integer> pids = runPageIds.get(minRun);

            Page curPage = buffer.getPage(pids.get(pIdx));
            rIdx++;
            if (rIdx >= curPage.getRecords().size()) {
                rIdx = 0;
                pIdx++;
            }
            pageIdx.set(minRun, pIdx);
            recordIdx.set(minRun, rIdx);
            heads.set(minRun, nextRecord(pids, pIdx, rIdx, buffer));
        }

        // drop run tables
        for (TableSchema run : runs) {
            ddl.dropTable(run.name());
        }

        return tempTable;
    }

    private Record nextRecord(List<Integer> pageIds, int pIdx, int rIdx, BufferManager buffer) throws DBException {
        if (pIdx >= pageIds.size()) return null;
        Page p = buffer.getPage(pageIds.get(pIdx));
        List<Record> records = p.getRecords();
        if (rIdx >= records.size()) return null;
        return records.get(rIdx);
    }
}
