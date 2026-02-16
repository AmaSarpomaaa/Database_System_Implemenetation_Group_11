package catalog;

import buffer.BufferManager;
import model.Page;
import model.Table;
import model.Datatype;
import model.TableSchema;
import model.Schema;
import model.Attribute;
import util.DBException;
import java.nio.ByteBuffer;
import java.util.*;

public class Catalog implements CatalogIN {
    private BufferManager bufferManager;

    // In-memory cache - just lookup by name
    private Map<String, TableSchema> tables;

    // System table page IDs
    private static final int SYS_TABLES_PAGE = 1;

    /**
     * Initialize the catalog with a buffer manager
     */
    public void initialize(BufferManager bufferManager) {
        this.bufferManager = bufferManager;
        this.tables = new HashMap<>();
    }

    /**
     * Load catalog from disk into memory cache
     */
    public void load() throws DBException {
        loadTables();
    }

    /**
     * Create empty system tables (for new database)
     */
    public void initializeSystemTables() throws DBException {
        Page sysTablesPage = bufferManager.getPage(SYS_TABLES_PAGE);
        sysTablesPage.initialize();
        bufferManager.markDirty(SYS_TABLES_PAGE);
    }

    /**
     * Load all tables from SYS_TABLES into memory
     */
    private void loadTables() throws DBException {
        Page sysTablesPage = bufferManager.getPage(SYS_TABLES_PAGE);

        int numRecords = sysTablesPage.getNumRecords();

        for (int slotId = 0; slotId < numRecords; slotId++) {
            byte[] recordData = sysTablesPage.getRecord(slotId);

            if (recordData != null) {
                TableSchema table = deserializeTable(recordData);
                tables.put(table.name(), table);
            }
        }
    }

    /**
     * Add a new table and persist to catalog
     */
    @Override
    public void addTable(Table table) throws DBException {
        TableSchema tableSchema = (TableSchema) table;

        // Add to in-memory cache
        tables.put(tableSchema.name(), tableSchema);

        // Persist to SYS_TABLES
        Page sysTablesPage = bufferManager.getPage(SYS_TABLES_PAGE);
        byte[] tableData = serializeTable(tableSchema);
        sysTablesPage.insertRecord(tableData);
        bufferManager.markDirty(SYS_TABLES_PAGE);
    }

    /**
     * Get table by name
     */
    @Override
    public TableSchema getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * Check if table exists
     */
    @Override
    public boolean exists(String tableName) {
        return tables.containsKey(tableName);
    }

    /**
     * Get all table names
     */
    public Set<String> getAllTableNames() {
        return tables.keySet();
    }

    /**
     * Drop a table
     */
    @Override
    public void dropTable(String tableName) throws DBException {
        if (!tables.containsKey(tableName)) {
            throw new DBException("Table " + tableName + " does not exist");
        }

        // Remove from cache
        tables.remove(tableName);
    }

    /**
     * Serialize table to bytes
     */
    /**
     * Serialize table to bytes
     */
    private byte[] serializeTable(TableSchema table) {
        ByteBuffer buffer = ByteBuffer.allocate(4096);

        // Write table name
        byte[] nameBytes = table.name().getBytes();
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);

        // Write schema (list of attributes)
        List<Attribute> attributes = table.schema().getAttributeList();
        buffer.putInt(attributes.size());

        for (Attribute attr : attributes) {
            // Write attribute name
            byte[] attrNameBytes = attr.getName().getBytes();
            buffer.putInt(attrNameBytes.length);
            buffer.put(attrNameBytes);

            // Write not null and unique flags
            buffer.put((byte) (attr.isNotNull() ? 1 : 0));
            buffer.put((byte) (attr.isUnique() ? 1 : 0));

            // Write attribute type (as enum ordinal)
            buffer.putInt(attr.getType().ordinal());
        }

        byte[] result = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(result);
        return result;
    }

    /**
     * Deserialize table from bytes
     */
    private TableSchema deserializeTable(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Read table name
        int nameLength = buffer.getInt();
        byte[] nameBytes = new byte[nameLength];
        buffer.get(nameBytes);
        String tableName = new String(nameBytes);

        // Read schema
        int numAttributes = buffer.getInt();
        List<Attribute> attributes = new ArrayList<>();

        for (int i = 0; i < numAttributes; i++) {
            // Read attribute name
            int attrNameLength = buffer.getInt();
            byte[] attrNameBytes = new byte[attrNameLength];
            buffer.get(attrNameBytes);
            String attrName = new String(attrNameBytes);

            // Read not null and unique flags
            boolean notNull = buffer.get() == 1;
            boolean unique = buffer.get() == 1;

            // Read attribute type (from enum ordinal)
            int typeOrdinal = buffer.getInt();
            Datatype attrType = Datatype.values()[typeOrdinal];

            attributes.add(new Attribute(attrName, notNull, unique, attrType));
        }

        Schema schema = new Schema(attributes);
        return new TableSchema(tableName, schema);
    }
}