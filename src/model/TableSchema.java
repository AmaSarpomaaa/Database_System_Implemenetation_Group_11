package model;

import buffer.BufferManager;
import index.BPlusTree;
import storage.StorageManager;
import util.DBException;
import java.util.ArrayList;
import java.util.List;

public class TableSchema implements Table {

    private final String name;
    private final Schema schema;
    private boolean temporary;

    // Persist this list via FileCatalog
    private final List<Integer> pageIds = new ArrayList<>();

    // Bound at runtime so we can read/write pages
    private StorageManager storage;
    private BufferManager buffer;
    private BPlusTree index = null;
    private int indexRootPageId = -1;

    public TableSchema(String name, Schema schema, StorageManager storage, BufferManager buffer) {
        this.name    = name;
        this.schema  = schema;
        this.storage = storage;
        this.buffer  = buffer;
        temporary    = false;
    }

    public TableSchema(String name, Schema schema, StorageManager storage,
                       BufferManager buffer, boolean temporary) {
        this.name      = name;
        this.schema    = schema;
        this.temporary = temporary;
        this.storage   = storage;
        this.buffer    = buffer;
    }

    public TableSchema(String name, Schema schema, List<Integer> pageIds) {
        this.name   = name;
        this.schema = schema;
        if (pageIds != null) this.pageIds.addAll(pageIds);
    }

    public void bind(StorageManager storage, BufferManager buffer) {
        this.storage = storage;
        this.buffer  = buffer;
    }

    public void buildIndex(int maxKeys) throws DBException {
        if (storage == null || buffer == null)
            throw new DBException("Table not bound to storage/buffer");

        Attribute pk = schema.getPrimaryKey();
        if (pk == null) throw new DBException("Cannot index a table with no primary key");

        index           = new BPlusTree(maxKeys, storage, buffer);
        indexRootPageId = index.getRootPageId();

        int pkIdx = schema.getAttributeIndex(pk.getName());

        for (int pid : pageIds) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                Object raw = r.getAttributes().get(pkIdx).getRaw();
                @SuppressWarnings("unchecked")
                Comparable<Object> key = (Comparable<Object>) raw;
                index.insert(key, pid);
                indexRootPageId = index.getRootPageId(); // update in case root split
            }
        }
    }

    public void openIndex(int rootPageId, int maxKeys) {
        this.indexRootPageId = rootPageId;
        this.index           = new BPlusTree(rootPageId, maxKeys, storage, buffer);
    }

    public BPlusTree getIndex() { return index; }

    public int getIndexRootPageId() { return indexRootPageId; }

    public List<Integer> getPageIds() { return pageIds; }

    @Override public String  name() { return name; }

    @Override public Schema  schema() { return schema; }

    public boolean isTemporary() { return temporary; }

    @Override
    public void insert(Record record) throws DBException {
        insert(record, false);
    }

    public void insert(Record record, boolean allowDup) throws DBException {
        if (storage == null || buffer == null)
            throw new DBException("Table not bound to storage/buffer");

        schema.validate(record);
        Attribute pk = schema.getPrimaryKey();
        if (pk == null && !allowDup)
            throw new DBException("Table has no primary key");

        int    pkIndex  = schema.getAttributeIndex(pk.getName());
        Object pkValue  = record.getAttributes().get(pkIndex).getRaw();

        // Check duplicates and find the right insertion page
        for (int i = 0; i < pageIds.size(); i++) {
            int          pid     = pageIds.get(i);
            Page         p       = buffer.getPage(pid);
            List<Record> records = p.getRecords();

            for (Record existing : records) {
                Object existingPk = existing.getAttributes().get(pkIndex).getRaw();
                if (pkValue != null && pkValue.equals(existingPk) && !allowDup)
                    throw new DBException("duplicate primary key value: ( " + pkValue + " )");
            }

            boolean isLastPage = (i == pageIds.size() - 1);
            Object  lastPk     = records.isEmpty() ? null
                    : records.get(records.size() - 1).getAttributes().get(pkIndex).getRaw();

            if (records.isEmpty() || compareKeys(pkValue, lastPk) <= 0 || isLastPage) {
                if (buffer.canFitRecord(p, record)) {
                    insertIntoSortedPosition(records, record, pkIndexCol);
                    buffer.markDirty(pid);
                    updateIndex(pkValue, pid);  // index update
                    return;
                }
                else{
                    insertIntoSortedPosition(records, record, pkIndexCol);
                    splitPage(i, p);
                    buffer.markDirty(pid);

                    // Page split may have moved records
                    if (index != null) {
                        rebuildIndexForPage(pageIds.get(i),   pkIndex);
                        rebuildIndexForPage(pageIds.get(i + 1), pkIndex);
                        indexRootPageId = index.getRootPageId();
                    }
                    return;
                }
            }
        }

        // No pages yet so allocate the first one
        int  pid     = storage.allocatePage();
        pageIds.add(pid);
        Page newPage = buffer.getPage(pid);
        newPage.addRecord(record);
        buffer.markDirty(pid);
        updateIndex(pkValue, pid);
    }

    private void updateIndex(Object pkValue, int dataPageId) throws DBException {
        if (index == null) return;
        @SuppressWarnings("unchecked")
        Comparable<Object> key = (Comparable<Object>) pkValue;
        index.delete(key);               // remove stale entry if present
        index.insert(key, dataPageId);
        indexRootPageId = index.getRootPageId();
    }

    private void rebuildIndexForPage(int pid, int pkIndex) throws DBException {
        Page         p       = buffer.getPage(pid);
        List<Record> records = p.getRecords();
        for (Record r : records) {
            Object raw = r.getAttributes().get(pkIndex).getRaw();
            @SuppressWarnings("unchecked")
            Comparable<Object> key = (Comparable<Object>) raw;
            index.delete(key);
            index.insert(key, pid);
        }
    }

    private void insertIntoSortedPosition(List<Record> records, Record record, int pkIndex) {
        Object newPk = record.getAttributes().get(pkIndex).getRaw();
        for (int i = 0; i < records.size(); i++) {
            Object currentPk = records.get(i).getAttributes().get(pkIndex).getRaw();
            if (compareKeys(newPk, currentPk) < 0) {
                records.add(i, record);
                return;
            }
        }
        records.add(record);
    }

    @SuppressWarnings("unchecked")
    private int compareKeys(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        if (a.getClass().equals(b.getClass()) && a instanceof Comparable)
            return ((Comparable<Object>) a).compareTo(b);
        return a.toString().compareTo(b.toString());
    }

    private void splitPage(int pageIndex, Page page) throws DBException {
        int  newPid   = storage.allocatePage();
        Page newPage  = buffer.getPage(newPid);
        int  mid      = page.size() / 2;
        while (page.size() > mid) {
            Record moved = page.removeRecordAt(mid);
            newPage.addRecord(moved);
        }
        pageIds.add(pageIndex + 1, newPid);
        buffer.markDirty(page.getPageID());
        buffer.markDirty(newPid);
    }

    @Override
    public List<Record> scan() throws DBException {
        if (storage == null || buffer == null)
            throw new DBException("Table not bound to storage/buffer");
        List<Record> result = new ArrayList<>();
        for (int pid : pageIds) {
            Page p = buffer.getPage(pid);
            result.addAll(p.getRecords());
        }
        return result;
    }

    public void append(Record record) throws DBException {
        if (pageIds.isEmpty()) {
            int pid = storage.allocatePage();
            pageIds.add(pid);
        }
        int  pid = pageIds.get(pageIds.size() - 1);
        Page p   = buffer.getPage(pid);
        if (buffer.canFitRecord(p, record)) {
            p.addRecord(record);
            buffer.markDirty(pid);
        } else {
            int  newPid  = storage.allocatePage();
            pageIds.add(newPid);
            Page newPage = buffer.getPage(newPid);
            newPage.addRecord(record);
            buffer.markDirty(newPid);
        }
    }

    private index.BPlusTree pkIndex;
    private boolean indexingEnabled;
    public index.BPlusTree getPkIndex() {
        return pkIndex;
    }

    public void bind(StorageManager storage, BufferManager buffer, boolean indexingEnabled) {
        this.storage = storage;
        this.buffer = buffer;
        this.indexingEnabled = indexingEnabled;
        buildIndex();
    }
    public void buildIndex() {
        if (!indexingEnabled) return;

        Attribute pk = schema.getPrimaryKey();
        if (pk != null) {
            pkIndex = new index.BPlusTree(4);
            int pkIndexCol = schema.getAttributeIndex(pk.getName());
            try {
                for (int pid : pageIds) {
                    Page p = buffer.getPage(pid);
                    List<Record> records = p.getRecords();
                    for (int i = 0; i < records.size(); i++) {
                        Comparable<Object> key = (Comparable<Object>) records.get(i).getAttributes().get(pkIndexCol).getRaw();
                        pkIndex.insert(key, new Record_ID(pid, i));
                    }
                }
            } catch (DBException e) {
                e.printStackTrace();
            }
        }
    }

    public void updateIndexForPage(int pid) throws DBException {
        if (pkIndex == null || !indexingEnabled) return;

        Page p = buffer.getPage(pid);
        int pkCol = schema.getAttributeIndex(schema.getPrimaryKey().getName());
        List<Record> records = p.getRecords();
        for (int i = 0; i < records.size(); i++) {
            Comparable<Object> key = (Comparable<Object>) records.get(i).getAttributes().get(pkCol).getRaw();
            pkIndex.delete(key);
            pkIndex.insert(key, new Record_ID(pid, i));
        }
    }
}