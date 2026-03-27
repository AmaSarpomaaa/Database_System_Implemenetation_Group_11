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
                ts.bind(storage, buffer);
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
        if (cmd instanceof DeleteCommand) return handleDelete((DeleteCommand) cmd);

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

    private Result handleDelete(DeleteCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        Table t = catalog.getTable(tableName);

        int deleted = 0;

        if (t instanceof TableSchema ts) {
            for (int pid : ts.getPageIds()) {
                Page p = buffer.getPage(pid);

                List<Record> records = p.getRecords();

                for (int i = records.size() - 1; i >= 0; i--) {
                    Record r = records.get(i);

                    if (matchesCondition(cmd, r, ts.schema())) {
                        records.remove(i);
                        deleted++;
                    }
                }

                buffer.markDirty(pid);
            }
        }

        return Result.ok(deleted + " rows deleted");
    }

    private Result handleUpdate(UpdateCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        Table t = catalog.getTable(tableName);

        int updated = 0;

        if (t instanceof TableSchema ts) {
            Schema schema = ts.schema();
            int attrIndex = schema.getAttributeIndex(cmd.getAttribute());

            for (int pid : ts.getPageIds()) {
                Page p = buffer.getPage(pid);

                for (Record r : p.getRecords()) {

                    if (matchesCondition(cmd, r, schema)) {

                        Object newValue = cmd.getValue();
                        r.getAttributes().set(attrIndex, new Value(newValue));
                        updated++;
                    }
                }

                buffer.markDirty(pid);
            }
        }

        return Result.ok(updated + " rows updated");
    }

    private boolean matchesCondition(Object cmd, Record r, Schema schema) {

        if (cmd instanceof DeleteCommand dc &&
                (dc.getConditions() == null || dc.getConditions().isEmpty())) return true;

        if (cmd instanceof UpdateCommand uc &&
                (uc.getConditions() == null || uc.getConditions().isEmpty())) return true;

        List<Condition> conditions =
                (cmd instanceof DeleteCommand dc) ? dc.getConditions() : ((UpdateCommand) cmd).getConditions();

        for (Condition c : conditions) {
            int index = schema.getAttributeIndex(c.getAttribute());
            Object value = r.getAttributes().get(index).getRaw();

            if (!value.equals(c.getValue())) {
                return false;
            }
        }

        return true;
    }

    private void print_helper(Table t, SelectCommand s) throws DBException {
        Schema schema = t.schema();
        List<Attribute> allAttrs = schema.getAttributes();

        // Determine which column indices to print
        List<Integer> colIndices = new ArrayList<>();
        if (s == null || s.isSelectStar()) {
            for (int i = 0; i < allAttrs.size(); i++) colIndices.add(i);
        } else {
            for (String[] pair : s.getAttributeNames()) {
                String attrName = pair[1];
                for (int i = 0; i < allAttrs.size(); i++) {
                    if (allAttrs.get(i).getName().equalsIgnoreCase(attrName)) {
                        colIndices.add(i);
                        break;
                    }
                }
            }
        }

        int colCount = colIndices.size();
        int[] widths = new int[colCount];
        for (int i = 0; i < colCount; i++)
            widths[i] = getColumnWidth(allAttrs.get(colIndices.get(i)));

        // Build divider
        StringBuilder divider = new StringBuilder("+");
        for (int w : widths) divider.append("-".repeat(w + 2)).append("+");

        // Print header
        System.out.println(divider);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < colCount; i++)
            header.append(String.format(" %-" + widths[i] + "s |", allAttrs.get(colIndices.get(i)).getName()));
        System.out.println(header);
        System.out.println(divider);

        // Single scan, print each record immediately
        for (model.Record r : t.scan()) {
            StringBuilder row = new StringBuilder("|");
            for (int i = 0; i < colCount; i++) {
                Value v = r.getAttributes().get(colIndices.get(i));
                String cell = (v == null || v.getRaw() == null) ? "NULL" : v.getRaw().toString();
                row.append(String.format(" %-" + widths[i] + "s |", cell));
            }
            System.out.println(row);
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