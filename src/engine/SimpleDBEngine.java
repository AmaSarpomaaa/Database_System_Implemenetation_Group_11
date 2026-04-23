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

import java.util.*;

public class SimpleDBEngine implements DBEngine {

    private StorageManager storage;
    private BufferManager  buffer;
    private Catalog        catalog;

    @Override
    public void startup(String dbLocation, int pageSize, int bufferSize,
                        boolean indexingEnabled) throws DBException {

        java.io.File dir = new java.io.File(dbLocation);
        if (!dir.exists()) dir.mkdirs();

        storage = new FileStorageManager();
        storage.open(dbLocation + "/database.db", pageSize);

        // maxKeys: how many keys fit comfortably in one index page.
        // pageSize / 10 is conservative and works for all key types.
        int indexMaxKeys = storage.getPageSize() / 10;

        FileCatalog fileCatalog = new FileCatalog(dbLocation + "/database.catalog");
        fileCatalog.setIndexMaxKeys(indexMaxKeys);  // NEW
        catalog = fileCatalog;
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

        DDLParser ddl = new DDLParser(catalog, storage, buffer);

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

            Table wTable = new TableSchema("w_table", fTable.schema(), storage, buffer);
            temp_tables.add(wTable);

            if (fTable instanceof TableSchema fts) {
                for (int pid : fts.getPageIds()) {
                    Page p = buffer.getPage(pid);
                    for (Record r : p.getRecords()) {
                        if (cmd.where(wTable.schema(), r)) wTable.insert(r);
                    }
                }
            } else {
                throw new DBException("Unsupported table type");
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

    private Result handleInsert(InsertCommand cmd) throws DBException {
        String tableName = cmd.getTableName();
        if (!catalog.exists(tableName)) return Result.error("No such table: " + tableName);

        Table t       = catalog.getTable(tableName);
        int inserted  = 0;

        for (List<Object[]> row : cmd.getValues()) {
            if (row == null || row.isEmpty()) continue;
            Record r = new Record();
            for (Object[] pair : row) r.addAttribute(new Value(pair[1]));
            try {
                t.insert(r);
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

        int       deleted  = 0;
        BPlusTree index    = ts.getIndex();
        Attribute pk       = ts.schema().getPrimaryKey();
        int       pkIndex  = (pk != null) ? ts.schema().getAttributeIndex(pk.getName()) : -1;

        for (int pid : ts.getPageIds()){
            Page p = buffer.getPage(pid);
            List<Record> records = p.getRecords();
            boolean changed = false;

            for (int i = records.size() - 1; i >= 0; i--){
                Record r = records.get(i);
                if (cmd.where(ts.schema(), r)) {
                    // remove from index before removing from page
                    if (index != null && pkIndex >= 0) {
                        Object raw = r.getAttributes().get(pkIndex).getRaw();
                        @SuppressWarnings("unchecked")
                        Comparable<Object> key = (Comparable<Object>) raw;
                        index.delete(key);
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
        if (!catalog.exists(tableName)) return Result.error("No such table: " + tableName);

        if (!(catalog.getTable(tableName) instanceof TableSchema ts))
            throw new DBException("Unsupported table type");

        Schema schema    = ts.schema();
        int    attrIndex = schema.getAttributeIndex(cmd.getAttribute());
        int    updated   = 0;

        for (int pid : ts.getPageIds()) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                if (cmd.where(schema, r)) {
                    Attribute attr = schema.getAttributes().get(attrIndex);
                    if (attr.isPrimaryKey()) {
                        Value newVal     = new Value(cmd.getValue());
                        int   matchCount = 0;
                        for (int checkPid : ts.getPageIds()) {
                            Page cp = buffer.getPage(checkPid);
                            for (Record cr : cp.getRecords())
                                if (cmd.where(schema, cr)) matchCount++;
                        }
                        if (matchCount > 1)
                            return Result.error("Cannot set multiple rows to the same primary key value: " + cmd.getValue());
                        for (int checkPid : ts.getPageIds()) {
                            Page cp = buffer.getPage(checkPid);
                            for (Record cr : cp.getRecords()) {
                                if (cr == r) continue;
                                if (cr.getAttributes().get(attrIndex).getRaw()
                                        .equals(newVal.getRaw()))
                                    return Result.error("Duplicate primary key value: " + newVal.getRaw());
                            }
                        }
                    }
                }
            }
        }

        // Apply pass
        BPlusTree index   = ts.getIndex();
        boolean  updatePK = schema.getAttributes().get(attrIndex).isPrimaryKey();

        for (int pid : ts.getPageIds()) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                if (cmd.where(schema, r)) {
                    // if this is a PK update, fix the index entry
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
            return rawValue; // already a typed literal (Integer, Double, null)
        }

        // Check if it's a plain attribute name
        int idx = schema.getAttributeIndex(expr);
        if (idx >= 0) {
            return record.getAttributes().get(idx).getRaw();
        }

        // Try to evaluate as a math expression: <operand> <op> <operand>
        // operand can be an attribute name or a numeric literal
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

        // Plain string literal fallback
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
}