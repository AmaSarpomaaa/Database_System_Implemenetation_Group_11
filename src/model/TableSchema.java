package model;

import buffer.BufferManager;
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

    // Used when CREATING a table at runtime
    public TableSchema(String name, Schema schema, StorageManager storage, BufferManager buffer) {
        this.name = name;
        this.schema = schema;
        this.storage = storage;
        this.buffer = buffer;
        temporary = false;
    }

    public TableSchema(String name, Schema schema, StorageManager storage, BufferManager buffer, boolean temporary) {
        this.name = name;
        this.schema = schema;
        this.temporary = temporary;
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

    public boolean isTemporary() {
        return temporary;
    }
    @Override
    public void insert(Record record) throws DBException {
        insert(record, false);
    }

    public void insert(Record record, boolean allowDup) throws DBException{
        if (storage == null || buffer == null){
            throw new DBException("Table not bound to storage/buffer");
        }
        schema.validate(record);
        Attribute pk = schema.getPrimaryKey();
        if (pk == null && !allowDup){
            throw new DBException("Table has no primary key");
        }
        int pkIndexCol = schema.getAttributeIndex(pk.getName());
        Object pkValue = record.getAttributes().get(pkIndexCol).getRaw();

        if (indexingEnabled && pkIndex != null && pkValue != null && !allowDup){
            if (pkIndex.search((Comparable<Object>) pkValue) != null){
                throw new DBException("duplicate primary key value: ( " + pkValue + " )");
            }
        }

        for (int i = 0; i < pageIds.size(); i++){
            int pid = pageIds.get(i);
            Page p = buffer.getPage(pid);
            List<Record> records = p.getRecords();

            if (!indexingEnabled || pkIndex == null){
                for (Record existing : records){
                    Object existingPk = existing.getAttributes().get(pkIndexCol).getRaw();
                    if (pkValue != null && pkValue.equals(existingPk) && !allowDup){
                        throw new DBException("duplicate primary key value: ( " + pkValue + " )");
                    }
                }
            }

            boolean isLastPage = (i == pageIds.size() - 1);
            Object lastPk = records.isEmpty() ? null : records.get(records.size() - 1).getAttributes().get(pkIndexCol).getRaw();

            if (records.isEmpty() || compareKeys(pkValue, lastPk) <= 0 || isLastPage){
                if (buffer.canFitRecord(p, record)) {
                    insertIntoSortedPosition(records, record, pkIndexCol);
                    buffer.markDirty(pid);
                    updateIndexForPage(pid);
                    return;
                }
                else{
                    insertIntoSortedPosition(records, record, pkIndexCol);
                    splitPage(i, p);
                    buffer.markDirty(pid);
                    updateIndexForPage(pid);
                    updateIndexForPage(pageIds.get(i + 1));
                    return;
                }
            }
        }


        int pid = storage.allocatePage();
        pageIds.add(pid);
        Page newPage = buffer.getPage(pid);
        newPage.addRecord(record);
        buffer.markDirty(pid);
        updateIndexForPage(pid);
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

        if (a.getClass().equals(b.getClass()) && a instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }

        return a.toString().compareTo(b.toString());
    }

    private void splitPage(int pageIndex, Page page) throws DBException {
        int newPid = storage.allocatePage();
        Page newPage = buffer.getPage(newPid);

        int mid = page.size() / 2;

        // move second half into new page
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

    public void append(Record record) throws DBException {
        if (pageIds.isEmpty()) {
            int pid = storage.allocatePage();
            pageIds.add(pid);
        }
        int pid = pageIds.get(pageIds.size() - 1);
        Page p = buffer.getPage(pid);
        if (buffer.canFitRecord(p, record)) {
            p.addRecord(record);
            buffer.markDirty(pid);
        } else {
            int newPid = storage.allocatePage();
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