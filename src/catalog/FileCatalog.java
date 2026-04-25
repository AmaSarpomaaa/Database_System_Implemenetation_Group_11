package catalog;

import buffer.BufferManager;
import model.*;
import storage.StorageManager;
import util.DBException;

import java.io.*;
import java.util.*;

public class FileCatalog implements Catalog {

    private final File catalogFile;
    private final Map<String, Table> tables = new HashMap<>();

    private final Map<String, int[]> savedIndexRoots = new HashMap<>();

    private int indexMaxKeys = 40;

    public FileCatalog(String dbLocation) {
        this.catalogFile = new File(dbLocation + ".catalog");
    }

    public void setIndexMaxKeys(int maxKeys) {
        this.indexMaxKeys = maxKeys;
    }

    @Override
    public void load() throws DBException {
        tables.clear();
        savedIndexRoots.clear();

        if (!catalogFile.exists() || catalogFile.length() == 0) return;

        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(catalogFile)))) {

            int tableCount = in.readInt();

            for (int t = 0; t < tableCount; t++) {
                String tableName = in.readUTF();

                int attrCount = in.readInt();
                List<Attribute> attrs = new ArrayList<>();
                for (int i = 0; i < attrCount; i++) {
                    String attrName = in.readUTF();
                    boolean notNull = in.readBoolean();
                    boolean primaryKey = in.readBoolean();
                    boolean unique = in.readBoolean();
                    Datatype type = Datatype.valueOf(in.readUTF());
                    int dataLength = in.readInt();

                    attrs.add(new Attribute(attrName, notNull, primaryKey, unique, type, dataLength));
                }

                Schema schema = new Schema(attrs);

                int pageCount = in.readInt();
                List<Integer> pageIds = new ArrayList<>();
                for (int i = 0; i < pageCount; i++) pageIds.add(in.readInt());

                // Read index root page IDs
                int[] indexRootPageIds = new int[attrCount];
                for (int i = 0; i < indexRootPageIds.length; i++) {
                    indexRootPageIds[i] = in.readInt();
                }

                tables.put(tableName.toLowerCase(), new TableSchema(tableName, schema, pageIds));

                savedIndexRoots.put(tableName.toLowerCase(), indexRootPageIds);
            }

        } catch (IOException e) {
            throw new DBException("Failed to load catalog from: " + catalogFile.getName(), e);
        }
    }

    @Override
    public void save() throws DBException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(catalogFile)))) {

            out.writeInt(tables.size());

            for (Table table : tables.values()) {
                out.writeUTF(table.name());

                List<Attribute> attrs = table.schema().getAttributes();
                out.writeInt(attrs.size());
                for (Attribute a : attrs) {
                    out.writeUTF(a.getName());
                    out.writeBoolean(a.isNotNull());
                    out.writeBoolean(a.isPrimaryKey());
                    out.writeBoolean(a.isUnique());
                    out.writeUTF(a.getType().name());
                    out.writeInt(a.getDataLength());
                }

                if (table instanceof TableSchema ts) {
                    List<Integer> pids = ts.getPageIds();
                    out.writeInt(pids.size());
                    for (int pid : pids) out.writeInt(pid);

                    // write index root page IDs
                    int[] rootPageIds = ts.getIndexRootPageIds();

                    for (int id : rootPageIds) {
                        out.writeInt(id);
                    }

                } else {
                    out.writeInt(0);   // pageCount
                    for (int i = 0; i < attrs.size(); i++) {
                        out.writeInt(-1); // indexRootPageIds
                    }

                }
            }

        } catch (IOException e) {
            throw new DBException("Failed to save catalog to: " + catalogFile.getName(), e);
        }
    }

    public void bind(StorageManager storage, BufferManager buffer) {
        for (Table t : tables.values()) {
            if (t instanceof TableSchema ts) {
                ts.bind(storage, buffer);
                // reopen the index if one was persisted
                int[] roots = savedIndexRoots.get(ts.name().toLowerCase());
                if (roots != null) {
                    ts.openIndices(roots, indexMaxKeys);
                }
            }
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
    public Map<String, Table> getTables() { return tables; }

    @Override
    public boolean exists(String tableName) {
        if (tableName == null) return false;
        return tables.containsKey(tableName.toLowerCase());
    }
}