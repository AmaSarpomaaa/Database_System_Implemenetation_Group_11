package engine;

import buffer.BufferManager;
import catalog.Catalog;
import catalog.FileCatalog;
import parser.ParserImplementation;
import storage.FileStorageManager;
import storage.StorageManager;
import util.DBException;
import ddl.DDLParser;
import model.*;
import util.ParseException;
import java.util.Map;

public class SimpleDBEngine implements DBEngine {

    private StorageManager storage;
    private BufferManager buffer;
    private Catalog catalog;

    @Override
    public void startup(String dbLocation, int pageSize, int bufferSize, boolean indexingEnabled) throws DBException {

        // 1) Storage Manager (disk)
        storage = new FileStorageManager();
        storage.open(dbLocation, pageSize);

        // 2) Catalog (table definitions)
        catalog = new FileCatalog(dbLocation);
        catalog.load();

        if (catalog instanceof FileCatalog fc) {
            fc.bind(storage, buffer);
        }
        Map<String, Table> fullListOfTables = catalog.getTables();
        for(String name: fullListOfTables.keySet()){
            Table associated = catalog.getTable(name);
            if(catalog instanceof FileCatalog fc){
                fc.bind(storage, buffer);
            }

        }


        // 3) Buffer Manager (RAM cache of pages)
        buffer = new BufferManager();
        buffer.initialize(bufferSize, storage.getPageSize(), storage);

        // Phase 1: indexingEnabled is accepted but can be ignored unless your phase requires it
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

    // Helpful getters if your processors need them (optional but useful)
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
        if (cmd instanceof SimpleSelectCommand) return handleSelect((SimpleSelectCommand) cmd);

        // ---------- INSERT ----------
        if (cmd instanceof InsertCommand) return handleInsert((InsertCommand) cmd);

        throw new DBException("Unsupported command in Phase 1 engine routing.");
    }

    private Result handleSelect(SimpleSelectCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        Table t = catalog.getTable(tableName);
        Schema s = t.schema();

        StringBuilder out = new StringBuilder();

        // Header
        out.append("\n|");
        for (Attribute a : s.getAttributes()) {
            out.append(" ").append(a.getName()).append(" |");
        }
        out.append("\n");

        // Divider (simple)
        int dashCount = Math.max(0, out.length() - 2);
        out.append("-".repeat(dashCount)).append("\n");


        java.util.List<model.Record> rows = new java.util.ArrayList<>();
        for (model.Record r : t.scan()) {
            rows.add(r);
        }


        Attribute pk = s.getPrimaryKey();
        int pkIndex = s.getAttributeIndex(pk.getName());

        rows.sort((r1, r2) -> {
            Object a = r1.getAttributes().get(pkIndex).getRaw();
            Object b = r2.getAttributes().get(pkIndex).getRaw();

            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;

            if (a instanceof Integer && b instanceof Integer)
                return ((Integer) a).compareTo((Integer) b);

            if (a instanceof Double && b instanceof Double)
                return ((Double) a).compareTo((Double) b);

            if (a instanceof String && b instanceof String)
                return ((String) a).compareTo((String) b);

            return a.toString().compareTo(b.toString());
        });

        for (model.Record r : rows) {
            out.append("|");
            for (Value v : r.getAttributes()) {
                Object raw = (v == null) ? null : v.getRaw();
                out.append(" ").append(raw == null ? "NULL" : raw).append(" |");
            }
            out.append("\n");
        }

        return Result.ok(out.toString());
    }

    private Result handleInsert(InsertCommand cmd) throws DBException {
        String tableName = cmd.getTableName();

        if (!catalog.exists(tableName)) {
            return Result.error("No such table: " + tableName);
        }

        Table t = catalog.getTable(tableName);
        int inserted = 0;

        // InsertCommand stores rows as List<Object[]>, and rows separated by addRow()
        for (java.util.List<Object[]> row : cmd.getValues()) {
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
}