package engine;

import buffer.BufferManager;
import catalog.Catalog;
import catalog.FileCatalog;
import index.BPlusTree;
import model.Record;
import parser.ParserImplementation;
import storage.FileStorageManager;
import storage.StorageManager;
import util.DBException;
import ddl.DDLParser;
import model.*;
import util.ParseException;
import parser.RelopNode;

import java.util.*;

public class SimpleDBEngine implements DBEngine {

    private StorageManager storage;
    private BufferManager  buffer;
    private Catalog        catalog;
    private boolean indexingEnabled;

    @Override
    public void startup(String dbLocation, int pageSize, int bufferSize,
                        boolean indexingEnabled) throws DBException {

        java.io.File dir = new java.io.File(dbLocation);
        this.indexingEnabled = indexingEnabled;
        if (!dir.exists()) dir.mkdirs();

        storage = new FileStorageManager();
        storage.open(dbLocation + "/database.db", pageSize);

        // maxKeys: how many keys fit comfortably in one index page.
        // pageSize / 10 is conservative and works for all key types.
        int indexMaxKeys = storage.getPageSize() / 10;

        FileCatalog fileCatalog = new FileCatalog(dbLocation + "/database.catalog");
        fileCatalog.setIndexMaxKeys(indexMaxKeys);
        catalog = fileCatalog;
        catalog.load();

        buffer = new BufferManager();
        buffer.initialize(bufferSize, storage.getPageSize(), storage);

        fileCatalog.bind(storage, buffer);

        if (indexingEnabled) {
            for (Map.Entry<String, Table> entry : catalog.getTables().entrySet()) {
                if (entry.getValue() instanceof TableSchema ts && ts.getIndex() == null) {
                    try {
                        ts.buildIndex(indexMaxKeys);
                    } catch (DBException ignored) {
                        // Table has no primary key
                    }
                }
            }
        }
    }

    @Override
    public void shutdown() throws DBException {
        // Save schemas and updated indexRootPageIds to the catalog file
        if (catalog != null) catalog.save();

        // Flush all data pages and index pages to disk
        if (buffer != null) buffer.flushAll();

        // Close the database file
        if (storage != null) storage.close();
    }

    public Catalog        getCatalog() { return catalog; }
    public BufferManager  getBuffer()  { return buffer;  }
    public StorageManager getStorage() { return storage; }

    public Result execute(String statement) throws DBException {
        ParsedCommand cmd;
        try {
            cmd = new ParserImplementation().parse(statement);
        } catch (ParseException e) {
            throw new DBException(e.getMessage());
        }

        DDLParser ddl = new DDLParser(catalog, storage, buffer, indexingEnabled);

        if (cmd instanceof CreateTableCommand)    return ddl.createTable((CreateTableCommand) cmd);
        if (cmd instanceof DropTableCommand)      return ddl.dropTable((DropTableCommand) cmd);
        if (cmd instanceof AlterTableAddCommand)  return ddl.alterTableAdd((AlterTableAddCommand) cmd);
        if (cmd instanceof AlterTableDropCommand) return ddl.alterTableDrop((AlterTableDropCommand) cmd);

        if (cmd instanceof SelectCommand) return handleSelect((SelectCommand) cmd, ddl);
        if (cmd instanceof InsertCommand) return handleInsert((InsertCommand) cmd);
        if (cmd instanceof DeleteCommand) return handleDelete((DeleteCommand) cmd, ddl);
        if (cmd instanceof UpdateCommand) return handleUpdate((UpdateCommand) cmd);

        throw new DBException("Unsupported command.");
    }

    private Result handleSelect(SelectCommand cmd, DDLParser ddl) throws DBException {
        ArrayList<Table> temp_tables = new ArrayList<>();
        try {
            for (String name : cmd.getTableNames()) {
                if (!catalog.exists(name)) return Result.error("No such table: " + name);
            }

            Table fTable = cmd.from(catalog, storage, buffer, ddl);
            temp_tables.add(fTable);
            Table wTable;

            if (cmd.hasWhere()) {
                wTable = new TableSchema("w_table", fTable.schema(), storage, buffer);
                temp_tables.add(wTable);

                if (fTable instanceof TableSchema fts) {
                    BPlusTree index = indexingEnabled ? fts.getIndex() : null;
                    Object pkVal    = (index != null) ? extractPKEqualityValue(cmd, fts) : null;

                    if (pkVal != null) {
                        int pid = index.search((Comparable<Object>) pkVal);
                        // Index lookup — only load the one page
                        if (pid != -1) {
                            Page p = buffer.getPage(pid);
                            for (Record r : p.getRecords()) {
                                if (cmd.where(wTable.schema(), r)) wTable.insert(indexingEnabled, r);
                            }
                        }
                    } else {
                        // Full scan fallback
                        for (int pid : fts.getPageIds()) {
                            Page p = buffer.getPage(pid);
                            for (Record r : p.getRecords()) {
                                if (cmd.where(wTable.schema(), r)) wTable.insert(indexingEnabled, r);
                            }
                        }
                    }
                } else {
                    throw new DBException("Unsupported table type");
                }
            }
            else {
                wTable = fTable;
            }

            Table oTable = cmd.orderBy(wTable, catalog, storage, buffer, ddl);
            temp_tables.add(oTable);
            print_helper(oTable, cmd);

        } finally {
            for (Table t : temp_tables) {
                if (t.isTemporary()) ddl.dropTable(t.name());
            }
        }
        return Result.ok(null);
    }

    private Object extractPKEqualityValue(SelectCommand cmd, TableSchema ts) {
        if (!(cmd.whereTree instanceof RelopNode relop)) return null;
        Attribute pk = ts.schema().getPrimaryKey();
        if (pk == null) return null;
        return relop.getEqualityValue(pk.getName());
    }

    private Result handleInsert(InsertCommand cmd) throws DBException {
        String tableName = cmd.getTableName();
        if (!catalog.exists(tableName)) return Result.error("No such table: " + tableName);

        if (!(catalog.getTable(tableName) instanceof TableSchema ts))
            throw new DBException("Unsupported table type");

        int       inserted = 0;

        for (List<Object[]> row : cmd.getValues()) {
            if (row == null || row.isEmpty()) continue;
            Record r = new Record();
            for (Object[] pair : row) r.addAttribute(new Value(pair[1]));
            try {
                ts.insert(indexingEnabled, r, false);

                //Pretty sure this stuff is all redundant, because it's all handled in ts.insert
//                // Fast duplicate check via index
//                if (index != null && pkIdx >= 0) {
//
//                    List<Value> attributes = r.getAttributes();
//
//                    Object pkVal = r.getAttributes().get(pkIdx).getRaw();
//                    @SuppressWarnings("unchecked")
//                    int existingPid = index.search((Comparable<Object>) pkVal);
//                    if (existingPid != -1)
//                        throw new DBException("duplicate primary key value: ( " + pkVal + " )");
//                    ts.insert(indexingEnabled, r, true);  // index already checked, skip internal scan
//                } else {
//                    ts.insert(indexingEnabled, r, false); // no index, let TableSchema do the full scan check
//                }

                inserted++;
            } catch (DBException e) {
                return Result.ok("Error: " + e.getMessage()
                        + "\n" + inserted + " rows inserted successfully");
            }
        }
        return Result.ok(inserted + " rows inserted successfully");
    }

    private Result handleDelete(DeleteCommand cmd, DDLParser ddl) throws DBException {
        String tableName = cmd.getTableName();
        if (!catalog.exists(tableName)) return Result.error("No such table: " + tableName);

        if (!(catalog.getTable(tableName) instanceof TableSchema ts))
            throw new DBException("Unsupported table type");

        int       deleted = 0;
        BPlusTree index   = indexingEnabled ? ts.getIndex() : null;
        Attribute pk      = ts.schema().getPrimaryKey();
        int       pkIndex = (pk != null) ? ts.schema().getAttributeIndex(pk.getName()) : -1;

        // Fast path: WHERE pk == value — use index to find the exact page
        Object pkVal = (index != null && pkIndex >= 0)
                ? extractPKEqualityValue(cmd, ts) : null;

        if (pkVal != null) {
            @SuppressWarnings("unchecked")
            int pid = index.search((Comparable<Object>) pkVal);
            if (pid != -1) {
                Page         p       = buffer.getPage(pid);
                List<Record> records = p.getRecords();
                for (int i = records.size() - 1; i >= 0; i--) {
                    Record r = records.get(i);
                    if (cmd.where(ts.schema(), r)) {
                        index.delete((Comparable<Object>) pkVal);
                        records.remove(i);
                        deleted++;
                        buffer.markDirty(pid);
                    }
                }
            }
        } else {
            // Full scan fallback
            for (int pid : ts.getPageIds()) {
                Page         p       = buffer.getPage(pid);
                List<Record> records = p.getRecords();
                boolean      changed = false;
                for (int i = records.size() - 1; i >= 0; i--) {
                    Record r = records.get(i);
                    if (cmd.where(ts.schema(), r)) {
                        if (index != null && pkIndex >= 0) {
                            Object raw = r.getAttributes().get(pkIndex).getRaw();
                            index.delete((Comparable<Object>) raw);
                        }
                        records.remove(i);
                        deleted++;
                        changed = true;
                    }
                }
                if (changed) buffer.markDirty(pid);
            }
        }

        return Result.ok(deleted + " rows deleted");
    }

    private Result handleUpdate(UpdateCommand cmd) throws DBException {
        String tableName = cmd.getTableName();
        if (!catalog.exists(tableName)) return Result.error("No such table: " + tableName);

        if (!(catalog.getTable(tableName) instanceof TableSchema ts))
            throw new DBException("Unsupported table type");

        Schema    schema    = ts.schema();
        int       attrIndex = schema.getAttributeIndex(cmd.getAttribute());
        int       updated   = 0;
        BPlusTree index     = indexingEnabled ? ts.getIndex() : null;
        Attribute pk        = ts.schema().getPrimaryKey();
        int       pkIndex   = (pk != null) ? ts.schema().getAttributeIndex(pk.getName()) : -1;
        boolean   updatePK  = schema.getAttributes().get(attrIndex).isPrimaryKey();

        // Fast path: WHERE pk == value — use index to find the exact page
        Object pkVal = (index != null && pkIndex >= 0)
                ? extractPKEqualityValue(cmd, ts) : null;

        if (pkVal != null) {
            // Duplicate check for PK update
            if (updatePK) {
                @SuppressWarnings("unchecked")
                int dupPid = index.search((Comparable<Object>) cmd.getValue());
                if (dupPid != -1)
                    return Result.error("Duplicate primary key value: " + cmd.getValue());
            }

            @SuppressWarnings("unchecked")
            int pid = index.search((Comparable<Object>) pkVal);
            if (pid != -1) {
                Page p = buffer.getPage(pid);
                for (Record r : p.getRecords()) {
                    if (cmd.where(schema, r)) {
                        if (updatePK) {
                            @SuppressWarnings("unchecked")
                            Comparable<Object> oldKey = (Comparable<Object>) pkVal;
                            index.delete(oldKey);
                            @SuppressWarnings("unchecked")
                            Comparable<Object> newKey = (Comparable<Object>) cmd.getValue();
                            index.insert(newKey, pid);
                        }
                        r.getAttributes().set(attrIndex, new Value(cmd.getValue()));
                        updated++;
                    }
                }
                buffer.markDirty(pid);
            }
        } else {
            // Validation pass for PK duplicate check
            if (updatePK) {
                if (index != null) {
                    @SuppressWarnings("unchecked")
                    int dupPid = index.search((Comparable<Object>) cmd.getValue());
                    if (dupPid != -1)
                        return Result.error("Duplicate primary key value: " + cmd.getValue());
                } else {
                    // Full scan duplicate check
                    for (int pid : ts.getPageIds()) {
                        Page cp = buffer.getPage(pid);
                        for (Record cr : cp.getRecords()) {
                            if (cr.getAttributes().get(attrIndex).getRaw()
                                    .equals(cmd.getValue()))
                                return Result.error("Duplicate primary key value: " + cmd.getValue());
                        }
                    }
                }
            }

            // Full scan apply pass
            for (int pid : ts.getPageIds()) {
                Page    p       = buffer.getPage(pid);
                boolean changed = false;
                for (Record r : p.getRecords()) {
                    if (cmd.where(schema, r)) {
                        if (index != null && updatePK) {
                            Object raw = r.getAttributes().get(attrIndex).getRaw();
                            @SuppressWarnings("unchecked")
                            Comparable<Object> oldKey = (Comparable<Object>) raw;
                            index.delete(oldKey);
                            @SuppressWarnings("unchecked")
                            Comparable<Object> newKey = (Comparable<Object>) cmd.getValue();
                            index.insert(newKey, pid);
                        }
                        r.getAttributes().set(attrIndex, new Value(cmd.getValue()));
                        updated++;
                        changed = true;
                    }
                }
                if (changed) buffer.markDirty(pid);
            }
        }

        return Result.ok(updated + " rows updated");
    }

    private void print_helper(Table t, SelectCommand s) throws DBException {
        Schema          schema   = t.schema();
        List<Attribute> allAttrs = schema.getAttributes();

        List<Integer> colIndices = new ArrayList<>();
        if (s == null || s.isSelectStar()) {
            for (int i = 0; i < allAttrs.size(); i++) {
                String n = allAttrs.get(i).getName();
                if (!n.equals("__pk") && !n.endsWith(".__pk")) colIndices.add(i);
            }
        } else {
            for (String[] pair : s.getAttributeNames()) {
                String attrName = pair[1];
                int    found    = -1;
                for (int i = 0; i < allAttrs.size(); i++) {
                    if (allAttrs.get(i).getName().equalsIgnoreCase(attrName)
                            || allAttrs.get(i).getName().endsWith("." + attrName)) {
                        found = i;
                        break;
                    }
                }
                if (found == -1) throw new DBException("No such attribute: " + attrName);
                colIndices.add(found);
            }
        }

        int   colCount = colIndices.size();
        int[] widths   = new int[colCount];
        for (int i = 0; i < colCount; i++)
            widths[i] = getColumnWidth(allAttrs.get(colIndices.get(i)));

        StringBuilder divider = new StringBuilder("+");
        for (int w : widths) divider.append("-".repeat(w + 2)).append("+");

        System.out.println(divider);
        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < colCount; i++)
            header.append(String.format(" %-" + widths[i] + "s |",
                    allAttrs.get(colIndices.get(i)).getName()));
        System.out.println(header);
        System.out.println(divider);

        if (!(t instanceof TableSchema ts)) throw new DBException("Unsupported table type");
        for (int pid : ts.getPageIds()) {
            System.out.println(pid);
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                StringBuilder row = new StringBuilder("|");
                for (int i = 0; i < colCount; i++) {
                    Value  v    = r.getAttributes().get(colIndices.get(i));
                    String cell = (v == null || v.getRaw() == null) ? "NULL" : v.getRaw().toString();
                    row.append(String.format(" %-" + widths[i] + "s |", cell));
                }
                System.out.println(row);
            }
        }
        System.out.println(divider);
    }

    private int getColumnWidth(Attribute attr) {
        int typeWidth;
        switch (attr.getType()) {
            case CHAR: case VARCHAR: typeWidth = attr.getDataLength(); break;
            case INTEGER:            typeWidth = 11;  break;
            case DOUBLE:             typeWidth = 20;  break;
            case BOOLEAN:            typeWidth = 5;   break;
            default:                 typeWidth = 10;
        }
        return Math.max(attr.getName().length(), typeWidth);
    }

    private Object evaluateValue(Object rawValue, Schema schema, Record record) throws DBException {
        if (!(rawValue instanceof String expr)) {
            return rawValue;
        }

        int idx = schema.getAttributeIndex(expr);
        if (idx >= 0) {
            return record.getAttributes().get(idx).getRaw();
        }

        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("([\\w.]+)\\s*([+\\-*/])\\s*([\\w.]+)")
                .matcher(expr);

        if (m.matches()) {
            double left  = resolveNumeric(m.group(1), schema, record);
            double right = resolveNumeric(m.group(3), schema, record);
            double result = switch (m.group(2)) {
                case "+" -> left + right;
                case "-" -> left - right;
                case "*" -> left * right;
                case "/" -> {
                    if (right == 0) throw new DBException("Division by zero");
                    yield left / right;
                }
                default -> throw new DBException("Unknown operator: " + m.group(2));
            };
            if (result == Math.floor(result) && !Double.isInfinite(result)) {
                return (int) result;
            }
            return result;
        }

        return expr;
    }

    private double resolveNumeric(String token, Schema schema, Record record) throws DBException {
        int idx = schema.getAttributeIndex(token);
        if (idx >= 0) {
            Object val = record.getAttributes().get(idx).getRaw();
            if (val instanceof Number n) return n.doubleValue();
            throw new DBException("Attribute " + token + " is not numeric");
        }

        try {
            return Double.parseDouble(token);
        } catch (NumberFormatException e) {
            throw new DBException("Cannot resolve numeric value: " + token);
        }
    }
    private Object extractPKEqualityValue(DeleteCommand cmd, TableSchema ts) {
        if (!(cmd.getWhereTree() instanceof RelopNode relop)) return null;
        Attribute pk = ts.schema().getPrimaryKey();
        if (pk == null) return null;
        return relop.getEqualityValue(pk.getName());
    }

    private Object extractPKEqualityValue(UpdateCommand cmd, TableSchema ts) {
        if (!(cmd.getWhereTree() instanceof RelopNode relop)) return null;
        Attribute pk = ts.schema().getPrimaryKey();
        if (pk == null) return null;
        return relop.getEqualityValue(pk.getName());
    }
}