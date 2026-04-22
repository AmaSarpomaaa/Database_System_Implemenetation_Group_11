package engine;

import buffer.BufferManager;
import catalog.Catalog;
import catalog.FileCatalog;
import model.Record;
import parser.IWhereTree;
import parser.ParserImplementation;
import storage.FileStorageManager;
import storage.StorageManager;
import util.DBException;
import ddl.DDLParser;
import model.*;
import util.ParseException;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

public class SimpleDBEngine implements DBEngine {

    private StorageManager storage;
    private BufferManager buffer;
    private Catalog catalog;

    @Override
    public void startup(String dbLocation, int pageSize, int bufferSize, boolean indexingEnabled) throws DBException {

        java.io.File dir = new java.io.File(dbLocation);
        if (!dir.exists()) {
            dir.mkdirs();
        }



        storage = new FileStorageManager();
        storage.open(dbLocation +"/database.db", pageSize);

        catalog = new FileCatalog(dbLocation + "/database.catalog");
        catalog.load();



        buffer = new BufferManager();
        buffer.initialize(bufferSize, storage.getPageSize(), storage);

        Map<String, Table> tables = catalog.getTables();
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            if (entry.getValue() instanceof TableSchema ts) {
                ts.bind(storage, buffer, indexingEnabled);
            }
        }

    }

    @Override
    public void shutdown() throws DBException {

        // Save schemas first (catalog)
        if (catalog != null) {
            catalog.save();
        }

        // Flush dirty pages to disk
        if (buffer != null) {
            buffer.flushAll();
        }

        // Close database file
        if (storage != null) {
            storage.close();
        }
    }


    public Catalog getCatalog() { return catalog; }
    public BufferManager getBuffer() { return buffer; }
    public StorageManager getStorage() { return storage; }



    public Result execute(String statement) throws DBException {
        ParsedCommand cmd;
        try {
            cmd = new ParserImplementation().parse(statement);
        } catch (ParseException e) {
            throw new DBException(e.getMessage());
        }

        DDLParser ddl = new DDLParser(catalog, storage, buffer);

        // ---------- DDL ----------
        if (cmd instanceof CreateTableCommand) return ddl.createTable((CreateTableCommand) cmd);
        if (cmd instanceof DropTableCommand) return ddl.dropTable((DropTableCommand) cmd);
        if (cmd instanceof AlterTableAddCommand) return ddl.alterTableAdd((AlterTableAddCommand) cmd);
        if (cmd instanceof AlterTableDropCommand) return ddl.alterTableDrop((AlterTableDropCommand) cmd);

        // ---------- SELECT ----------
        if (cmd instanceof SelectCommand) return handleSelect((SelectCommand) cmd, ddl);

        // ---------- INSERT ----------
        if (cmd instanceof InsertCommand) return handleInsert((InsertCommand) cmd);

        // ---------- DELETE ----------
        if (cmd instanceof DeleteCommand) return handleDelete((DeleteCommand) cmd, ddl);

// ---------- UPDATE ----------
        if (cmd instanceof UpdateCommand) return handleUpdate((UpdateCommand) cmd);

        throw new DBException("Unsupported command.");
    }

    private Result handleSelect(SelectCommand cmd, DDLParser ddl) throws DBException {
        ArrayList<Table> temp_tables = new ArrayList<Table>();
        try {
            //error checking
            for (String name : cmd.getTableNames()) {
                if (!catalog.exists(name)) {
                    return Result.error("No such table: " + name);
                }
            }
            Table fTable = cmd.from(catalog, storage, buffer, ddl);
            temp_tables.add(fTable);

            //Where Table
            Table wTable = new TableSchema("w_table", fTable.schema(), storage, buffer);
            temp_tables.add(wTable);
            if (fTable instanceof TableSchema fts) {
                for (int pid : fts.getPageIds()) {
                    Page p = buffer.getPage(pid);
                    for (model.Record r : p.getRecords()) {
                        if (cmd.where(wTable.schema(), r)){
                            wTable.insert(r);
                        }
                    }
                }
            } else {
                throw new DBException("Unsupported table type");
            }

            Table oTable = cmd.orderBy(wTable, catalog, storage, buffer, ddl);
            temp_tables.add(oTable);

            print_helper(oTable,cmd);


        } finally{
            //Always drop temp tables
            for (Table t : temp_tables) {
                if (t.isTemporary())
                    ddl.dropTable(t.name());
            }
        }

        return Result.ok(null);
    }

    private Result handleInsert(InsertCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        Table t = catalog.getTable(tableName);
        int inserted = 0;

        // InsertCommand stores rows as List<Object[]>, and rows separated by addRow()
        for (List<Object[]> row : cmd.getValues()) {
            if (row == null || row.isEmpty()) continue;

            model.Record r = new model.Record();
            for (Object[] pair : row) {
                Object raw = pair[1];              // pair[0] is Datatype, pair[1] is the value
                r.addAttribute(new Value(raw));
            }

            try {
                t.insert(r);
                inserted++;
            } catch (DBException e) {
                return Result.ok("Error: " + e.getMessage() + "\n" + inserted + " rows inserted successfully");
            }
        }


        return Result.ok(inserted + " rows inserted successfully");


    }

    private Result handleDelete(DeleteCommand cmd, DDLParser ddl) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)){
            return Result.error("No such table: " + tableName);
        }

        if (!(catalog.getTable(tableName) instanceof TableSchema ts)){
            throw new DBException("Unsupported table type");
        }

        int deleted = 0;

        for (int pid : ts.getPageIds()){
            Page p = buffer.getPage(pid);
            List<Record> records = p.getRecords();
            boolean changed = false;

            for (int i = records.size() - 1; i >= 0; i--){
                Record r = records.get(i);
                if (cmd.where(ts.schema(), r)){
                    if (ts.getPkIndex() != null){
                        Attribute pk = ts.schema().getPrimaryKey();
                        int pkCol = ts.schema().getAttributeIndex(pk.getName());
                        Comparable<Object> pkVal = (Comparable<Object>) r.getAttributes().get(pkCol).getRaw();
                        ts.getPkIndex().delete(pkVal);
                    }
                    records.remove(i);
                    deleted++;
                    changed = true;
                }
            }
            if (changed){
                buffer.markDirty(pid);
                ts.updateIndexForPage(pid);
            }
        }

        return Result.ok(deleted + " rows deleted");
    }

    private Result handleUpdate(UpdateCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        if (!(catalog.getTable(tableName) instanceof TableSchema ts)) {
            throw new DBException("Unsupported table type");
        }

        Schema schema = ts.schema();
        int attrIndex = schema.getAttributeIndex(cmd.getAttribute());
        int updated = 0;

        for (int pid : ts.getPageIds()) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                if (cmd.where(schema, r)) {
                    Attribute attr = schema.getAttributes().get(attrIndex);
                    if (attr.isPrimaryKey()) {
                        Value newVal = new Value(cmd.getValue());
                        // check uniqueness against all records
                        if (attr.isPrimaryKey()) {
                            // count how many rows will be updated
                            int matchCount = 0;
                            for (int checkPid : ts.getPageIds()) {
                                Page checkPage = buffer.getPage(checkPid);
                                for (Record checkRec : checkPage.getRecords()) {
                                    if (cmd.where(schema, checkRec)) matchCount++;
                                }
                            }
                            if (matchCount > 1) {
                                return Result.error("Cannot set multiple rows to the same primary key value: " + cmd.getValue());
                            }
                            for (int checkPid : ts.getPageIds()) {
                                Page checkPage = buffer.getPage(checkPid);
                                for (Record checkRec : checkPage.getRecords()) {
                                    if (checkRec == r) continue;
                                    if (checkRec.getAttributes().get(attrIndex).getRaw().equals(newVal.getRaw())) {
                                        return Result.error("Duplicate primary key value: " + newVal.getRaw());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // all checks passed, now apply
        for (int pid : ts.getPageIds()){
            Page p = buffer.getPage(pid);
            boolean changed = false;

            for (Record r : p.getRecords()){
                if (cmd.where(schema, r)){
                    Attribute attr = schema.getAttributes().get(attrIndex);

                    if (attr.isPrimaryKey() && ts.getPkIndex() != null){
                        Comparable<Object> oldVal = (Comparable<Object>) r.getAttributes().get(attrIndex).getRaw();
                        ts.getPkIndex().delete(oldVal);
                    }

                    r.getAttributes().set(attrIndex, new Value(cmd.getValue()));

                    if (attr.isPrimaryKey() && ts.getPkIndex() != null){
                        Comparable<Object> newVal = (Comparable<Object>) r.getAttributes().get(attrIndex).getRaw();
                        int slotId = p.getRecords().indexOf(r);
                        ts.getPkIndex().insert(newVal, new Record_ID(pid, slotId));
                    }
                    updated++;
                    changed = true;
                }
            }
            if (changed){
                buffer.markDirty(pid);
            }
        }

        return Result.ok(updated + " rows updated");
    }



    private void print_helper(Table t, SelectCommand s) throws DBException {
        Schema schema = t.schema();
        List<Attribute> allAttrs = schema.getAttributes();

        // Determine which column indices to print
        List<Integer> colIndices = new ArrayList<>();
        if (s == null || s.isSelectStar()) {
            for (int i = 0; i < allAttrs.size(); i++) {
                if (!allAttrs.get(i).getName().equals("__pk") && !allAttrs.get(i).getName().endsWith(".__pk"))
                    colIndices.add(i);
            }
        } else {
            for (String[] pair : s.getAttributeNames()) {
                String attrName = pair[1];
                int found = -1;
                for (int i = 0; i < allAttrs.size(); i++) {
                    if (allAttrs.get(i).getName().equalsIgnoreCase(attrName) ||
                            allAttrs.get(i).getName().endsWith("." + attrName)) {
                        found = i;
                        break;
                    }
                }
                if (found == -1) {
                    throw new DBException("No such attribute: " + attrName);
                }
                colIndices.add(found);
            }
        }

        int colCount = colIndices.size();
        int[] widths = new int[colCount];
        for (int i = 0; i < colCount; i++)
            widths[i] = getColumnWidth(allAttrs.get(colIndices.get(i)));

        StringBuilder divider = new StringBuilder("+");
        for (int w : widths) divider.append("-".repeat(w + 2)).append("+");

        System.out.println(divider);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < colCount; i++)
            header.append(String.format(" %-" + widths[i] + "s |", allAttrs.get(colIndices.get(i)).getName()));
        System.out.println(header);
        System.out.println(divider);

        // Stream page by page instead of scan()
        if (!(t instanceof TableSchema ts)) {
            throw new DBException("Unsupported table type");
        }
        for (int pid : ts.getPageIds()) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                StringBuilder row = new StringBuilder("|");
                for (int i = 0; i < colCount; i++) {
                    Value v = r.getAttributes().get(colIndices.get(i));
                    String cell = (v == null || v.getRaw() == null) ? "NULL" : v.getRaw().toString();
                    row.append(String.format(" %-" + widths[i] + "s |", cell));
                }
                System.out.println(row);
            }
        }
        System.out.println(divider);
    }
    private int getColumnWidth(Attribute attr) {
        String name = attr.getName();
        int typeWidth;
        switch (attr.getType()) {
            case CHAR:
            case VARCHAR:
                typeWidth = attr.getDataLength();
                break;
            case INTEGER:
                typeWidth = 11; // max int digits + sign
                break;
            case DOUBLE:
                typeWidth = 20; // reasonable max for doubles
                break;
            case BOOLEAN:
                typeWidth = 5; // "false"
                break;
            default:
                typeWidth = 10;
        }
        return Math.max(name.length(), typeWidth);
    }
}