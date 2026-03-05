package model;

import buffer.BufferManager;
import storage.StorageManager;
import util.DBException;

import java.util.ArrayList;
import java.util.List;

public class TableSchema implements Table {

    private final String name;
    private final Schema schema;

    // Persist this list via FileCatalog
    private final List<Integer> pageIds = new ArrayList<>();

    // Bound at runtime so we can read/write pages
    private StorageManager storage;
    private BufferManager buffer;

    // Used when CREATING a table at runtime
    public TableSchema(String name, Schema schema, StorageManager storage, BufferManager buffer) {
        this.name = name;
        this.schema = schema;
        this.storage = storage;
        this.buffer = buffer;
    }

    // Used when LOADING from catalog file (bind storage/buffer later)
    public TableSchema(String name, Schema schema, List<Integer> pageIds) {
        this.name = name;
        this.schema = schema;
        if (pageIds != null) this.pageIds.addAll(pageIds);
    }

    // Called after catalog.load() in startup()
    public void bind(StorageManager storage, BufferManager buffer) {
        this.storage = storage;
        this.buffer = buffer;
    }

    public List<Integer> getPageIds() {
        return pageIds;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public void insert(Record record) throws DBException {
        if (storage == null || buffer == null) {
            throw new DBException("Table not bound to storage/buffer");
        }

        // Validate record size + types + not null
        schema.validate(record);

        // Primary key uniqueness (scan existing pages)
        Attribute pk = schema.getPrimaryKey();
        if (pk != null) {
            int pkIndex = schema.getAttributeIndex(pk.getName());
            Object pkValue = record.getAttributes().get(pkIndex).getRaw();

            for (int pid : pageIds) {
                Page p = buffer.getPage(pid);
                for (Record existing : p.getRecords()) {
                    Object existingPk = existing.getAttributes().get(pkIndex).getRaw();
                    if (pkValue != null && pkValue.equals(existingPk)) {
                        throw new DBException("duplicate primary key value: ( " + pkValue + " )");
                    }
                }
            }
        }

        // Phase 1 simplification: 1 record per page
        int pid = storage.allocatePage();
        pageIds.add(pid);  // ← moved to after PK check

        Page p = buffer.getPage(pid);
        p.addRecord(record);
        buffer.markDirty(pid);
    }


    @Override
    public List<Record> scan() throws DBException {
        if (storage == null || buffer == null) {
            throw new DBException("Table not bound to storage/buffer");
        }

        List<Record> result = new ArrayList<>();
        for (int pid : pageIds) {
            Page p = buffer.getPage(pid);
            result.addAll(p.getRecords());
        }
        return result;
    }
}