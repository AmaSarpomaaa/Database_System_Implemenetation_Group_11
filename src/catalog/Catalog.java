package catalog;

import buffer.BufferManager;
import model.TableSchema;
import model.Schema;
import util.DBException;
import java.util.*;

public class Catalog implements CatalogIN {
    private final Map<String, TableSchema> tables;
    private final BufferManager bufferManager;
    private int nextPageId = 0;

    public Catalog(BufferManager bufferManager) {
        this.tables = new LinkedHashMap<>();
        this.bufferManager = bufferManager;
    }

    public int allocatePageId() {
        return nextPageId++;
    }

    public void load() throws DBException {
        return;
    }

    public void save() throws DBException {
        return;
    }

    public void createTable(String name, Schema schema) throws DBException {
        if (hasTable(name))
            throw new DBException("Table already exists: " + name);
        tables.put(name.toLowerCase(), new TableSchema(name, schema, bufferManager, this));
    }

    public void dropTable(String name) throws DBException {
        if (tables.remove(name.toLowerCase()) == null)
            throw new DBException("Table not found: " + name);
    }

    public TableSchema getTable(String name) throws DBException {
        TableSchema table = tables.get(name.toLowerCase());
        if (table == null)
            throw new DBException("Table not found: " + name);
        return table;
    }

    public boolean hasTable(String name) {
        return tables.containsKey(name.toLowerCase());
    }

    public Collection<TableSchema> getTables() {
        return Collections.unmodifiableCollection(tables.values());
    }

    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }

    public void shutdown() throws DBException {
        bufferManager.flushAll();
    }
}



