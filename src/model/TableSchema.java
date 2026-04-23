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

        if (index == null) {
            try { buildIndex(storage.getPageSize() / 10); }
            catch (DBException ignored) {}
        }

        schema.validate(record);
        Attribute pk = schema.getPrimaryKey();
        if (pk == null && !allowDup)
            throw new DBException("Table has no primary key");

        int    pkIndex = schema.getAttributeIndex(pk.getName());
        Object pkValue = record.getAttributes().get(pkIndex).getRaw();

        // duplicate check if needed
        if (!allowDup) {
            if (index != null) {
                @SuppressWarnings("unchecked")
                int existingPid = index.search((Comparable<Object>) pkValue);
                if (existingPid != -1)
                    throw new DBException("duplicate primary key value: ( " + pkValue + " )");
            } else {
                for (int i = 0; i < pageIds.size(); i++) {
                    Page p = buffer.getPage(pageIds.get(i));
                    for (Record existing : p.getRecords()) {
                        Object existingPk = existing.getAttributes().get(pkIndex).getRaw();
                        if (pkValue != null && pkValue.equals(existingPk))
                            throw new DBException("duplicate primary key value: ( " + pkValue + " )");
                    }
                }
            }
        }

        // No pages yet — allocate first
        if (pageIds.isEmpty()) {
            int  pid     = storage.allocatePage();
            pageIds.add(pid);
            Page newPage = buffer.getPage(pid);
            newPage.addRecord(record);
            buffer.markDirty(pid);
            updateIndex(pkValue, pid);
            return;
        }

        // Use index to find the right page, fall back to last page
        int targetPid = -1;
        if (index != null) {
            // find the page of the largest key <= pkValue via range search
            // just use last page as target and let sorted insert handle it,
            // but skip the full scan by going straight to last page
        }

        // Fall back: just append to last page, split if needed
        int  lastIdx = pageIds.size() - 1;
        int  pid     = pageIds.get(lastIdx);
        Page p       = buffer.getPage(pid);

        if (buffer.canFitRecord(p, record)) {
            insertIntoSortedPosition(p.getRecords(), record, pkIndex);
            buffer.markDirty(pid);
            updateIndex(pkValue, pid);
        } else {
            insertIntoSortedPosition(p.getRecords(), record, pkIndex);
            splitPage(lastIdx, p);
            buffer.markDirty(pid);
            if (index != null) {
                rebuildIndexForPage(pageIds.get(lastIdx), pkIndex);
                rebuildIndexForPage(pageIds.get(lastIdx + 1), pkIndex);
                indexRootPageId = index.getRootPageId();
            }
        }
    }

    private void updateIndex(Object pkValue, int dataPageId) throws DBException {
        if (index == null) return;
        @SuppressWarnings("unchecked")
        Comparable<Object> key = (Comparable<Object>) pkValue;
        index.delete(key);               // remove stale entry if present
        index.insert(key, dataPageId);
        indexRootPageId = index.getRootPageId();
        // DEBUG: verify immediately after insert
        @SuppressWarnings("unchecked")
        int verify = index.search((Comparable<Object>) pkValue);
        if (verify == -1) {
            System.out.println("DEBUG updateIndex: FAILED to find key=" + pkValue + " after insert");
        }
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
            indexRootPageId = index.getRootPageId();
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

    public void bind(StorageManager storage, BufferManager buffer, boolean indexingEnabled) {
        this.storage = storage;
        this.buffer  = buffer;
        if (indexingEnabled) {
            try {
                buildIndex(storage.getPageSize() / 10);
            } catch (DBException ignored) {
                // Table has no primary key
            }
        }
    }
}