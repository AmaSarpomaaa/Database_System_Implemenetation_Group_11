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

        schema.validate(record);

        Attribute pk = schema.getPrimaryKey();
        if (pk == null) {
            throw new DBException("Table has no primary key");
        }
        int pkIndex = schema.getAttributeIndex(pk.getName());
        Object pkValue = record.getAttributes().get(pkIndex).getRaw();

        // check duplicates
        for (int i = 0; i < pageIds.size(); i++) {
            int pid = pageIds.get(i);
            Page p = buffer.getPage(pid);
            List<Record> records = p.getRecords();

            for (Record existing : records) {
                Object existingPk = existing.getAttributes().get(pkIndex).getRaw();
                if (pkValue != null && pkValue.equals(existingPk)) {
                    throw new DBException("duplicate primary key value: ( " + pkValue + " )");
                }
            }

            // if page is empty, insert here
            if (records.isEmpty()) {
                records.add(record);
                buffer.markDirty(pid);
                return;
            }

            // compare with last record in this page
            Object lastPk = records.get(records.size() - 1).getAttributes().get(pkIndex).getRaw();

            if (compareKeys(pkValue, lastPk) < 0) {
                // record belongs somewhere in this page
                if (buffer.canFitRecord(p, record)) {
                    insertIntoSortedPosition(records, record, pkIndex);
                    buffer.markDirty(pid);
                    return;
                } else {
                    insertIntoSortedPosition(records, record, pkIndex);
                    splitPage(i, p);
                    buffer.markDirty(pid);
                    return;
                }
            }
        }

        // try inserting into last page first
        if (!pageIds.isEmpty()) {
            int lastPid = pageIds.get(pageIds.size() - 1);
            Page lastPage = buffer.getPage(lastPid);

            if (buffer.canFitRecord(lastPage, record)) {
                insertIntoSortedPosition(lastPage.getRecords(), record, pkIndex);
                buffer.markDirty(lastPid);
                return;
            }else{
                insertIntoSortedPosition(lastPage.getRecords(), record, pkIndex);
                splitPage(pageIds.size() -1, lastPage);
                buffer.markDirty(lastPid);
                return;
            }

        }

        // if page full OR no pages yet, allocate new page
        int pid = storage.allocatePage();
        pageIds.add(pid);

        Page newPage = buffer.getPage(pid);
        newPage.addRecord(record);
        buffer.markDirty(pid);
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
}