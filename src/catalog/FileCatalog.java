package catalog;

import model.*;
import util.DBException;

import java.io.*;
import java.util.*;

/**
 * Phase 1 Catalog implementation:
 * - Keeps tables in memory during runtime
 * - Persists table schemas to a catalog file on shutdown
 * - Reloads them on startup
 *
 * Data pages/records are NOT persisted here (Phase 1 usually only requires schema/catalog persistence).
 */
public class FileCatalog implements Catalog {

    private final File catalogFile;
    private final Map<String, Table> tables = new HashMap<>();

    public FileCatalog(String dbLocation) {
        // You can pick any extension you like
        this.catalogFile = new File(dbLocation + ".catalog");
    }

    @Override
    public void load() throws DBException {
        tables.clear();

        if (!catalogFile.exists() || catalogFile.length() == 0) {
            return; // nothing to load
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(catalogFile)))) {

            int tableCount = in.readInt();

            for (int t = 0; t < tableCount; t++) {
                String tableName = in.readUTF();

                int attrCount = in.readInt();
                List<Attribute> attrs = new ArrayList<>();

                for (int i = 0; i < attrCount; i++) {
                    String attrName = in.readUTF();
                    boolean notNull = in.readBoolean();
                    boolean primaryKey = in.readBoolean();
                    Datatype type = Datatype.valueOf(in.readUTF());

                    attrs.add(new Attribute(attrName, notNull, primaryKey, type));
                }

                Schema schema = new Schema(attrs);

                // For Phase 1 you can rebuild an in-memory TableSchema.
                // Later you’ll create a real Table implementation that uses Buffer/Storage.
                Table table = new TableSchema(tableName, schema);

                tables.put(tableName.toLowerCase(), table);
            }

        } catch (IOException e) {
            throw new DBException("Failed to load catalog from: " + catalogFile.getName(), e);
        }
    }

    @Override
    public void save() throws DBException {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(catalogFile)))) {

            out.writeInt(tables.size());

            for (Table table : tables.values()) {
                out.writeUTF(table.name());

                List<Attribute> attrs = table.schema().getAttributes();
                out.writeInt(attrs.size());

                for (Attribute a : attrs) {
                    out.writeUTF(a.getName());
                    out.writeBoolean(a.isNotNull());
                    out.writeBoolean(a.isPrimaryKey());
                    out.writeUTF(a.getType().name());
                }
            }

        } catch (IOException e) {
            throw new DBException("Failed to save catalog to: " + catalogFile.getName(), e);
        }
    }

    @Override
    public void addTable(Table table) throws DBException {
        if (table == null) throw new DBException("Cannot add null table");
        String key = table.name().toLowerCase();
        if (tables.containsKey(key)) throw new DBException("Table already exists: " + table.name());
        tables.put(key, table);
    }

    @Override
    public void removeTable(String tableName) throws DBException {
        if (tableName == null) throw new DBException("Table name is null");
        String key = tableName.toLowerCase();
        if (!tables.containsKey(key)) throw new DBException("Table does not exist: " + tableName);
        tables.remove(key);
    }

    @Override
    public Table getTable(String tableName) throws DBException {
        if (tableName == null) throw new DBException("Table name is null");
        Table t = tables.get(tableName.toLowerCase());
        if (t == null) throw new DBException("Table does not exist: " + tableName);
        return t;
    }

    @Override
    public boolean exists(String tableName) {
        if (tableName == null) return false;
        return tables.containsKey(tableName.toLowerCase());
    }
}
