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
    //the primary key index
    private BPlusTree index = null;
    //unique value indexes
    private BPlusTree[] indexTrees;
    private int indexRootPageId = -1;

    public TableSchema(String name, Schema schema, StorageManager storage, BufferManager buffer) {
        this.name    = name;
        this.schema  = schema;
        this.storage = storage;
        this.buffer  = buffer;
        temporary    = false;
        indexTrees = new BPlusTree[schema.getAttributes().size()];
    }

    public TableSchema(String name, Schema schema, StorageManager storage,
                       BufferManager buffer, boolean temporary) {
        this.name      = name;
        this.schema    = schema;
        this.temporary = temporary;
        this.storage   = storage;
        this.buffer    = buffer;
        indexTrees = new BPlusTree[schema.getAttributes().size()];
    }

    public TableSchema(String name, Schema schema, List<Integer> pageIds) {
        this.name   = name;
        this.schema = schema;
        if (pageIds != null) this.pageIds.addAll(pageIds);
        indexTrees = new BPlusTree[schema.getAttributes().size()];
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

        index           = buildBPlusTree(maxKeys, pk);
        indexRootPageId = index.getRootPageId();

        //index unique values
        List<Attribute> attributes = schema.getAttributes();

        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = attributes.get(i);
            if (attribute.isPrimaryKey()) {
                indexTrees[i] = index;
            }
            else if (attribute.isUnique()) {
                indexTrees[i] = buildBPlusTree(maxKeys, attribute);
            }
            else {
                //don't build an index tree unless the attribute is unique or the primary key
                indexTrees[i] = null;
            }
        }

    }

    private BPlusTree buildBPlusTree(int maxKeys, Attribute attribute) throws DBException {
        if (storage == null || buffer == null)
            throw new DBException("Table not bound to storage/buffer");

        BPlusTree tree = new BPlusTree(maxKeys, storage, buffer);

        int attrIndex = schema.getAttributeIndex(attribute.getName());

        for (int pid : pageIds) {
            Page p = buffer.getPage(pid);
            for (Record r : p.getRecords()) {
                Object raw = r.getAttributes().get(attrIndex).getRaw();
                @SuppressWarnings("unchecked")
                Comparable<Object> key = (Comparable<Object>) raw;
                tree.insert(key, pid);
            }
        }

        return tree;

    }

    /**
     * Open the index on the primary key, given the id of its root page
     * @param rootPageId The root page id for the table of the primary key
     * @deprecated
     */
    @Deprecated
    public void openIndex(int rootPageId, int maxKeys) {
        this.indexRootPageId = rootPageId;
        this.index           = new BPlusTree(rootPageId, maxKeys, storage, buffer);
    }

    public void openIndices(int[] rootPageIds, int maxKeys) {

        List<Attribute> attributes = schema.getAttributes();

        for (int i = 0; i < rootPageIds.length; i++) {

            int rootPageId = rootPageIds[i];

            if (rootPageId != -1) {
                if (attributes.get(i).isPrimaryKey()) {
                    this.indexRootPageId = rootPageId;
                    this.index           = new BPlusTree(rootPageId, maxKeys, storage, buffer);
                    indexTrees[i]        = index;
                }
                else {
                    indexTrees[i] = new BPlusTree(rootPageId, maxKeys, storage, buffer);
                }
            }
        }
    }

    /**
     * @return the index on the primary key
     */
    public BPlusTree getIndex() { return index; }

    public BPlusTree[] getIndices() {
        return indexTrees;
    }

    /**
     * @return the root page id of the index tree associated with the primarykey
     * @deprecated
     */
    @Deprecated
    public int getIndexRootPageId() { return indexRootPageId; }

    public int[] getIndexRootPageIds() {
        int[] ids = new int[indexTrees.length];

        for (int i = 0; i < indexTrees.length; i++) {
            BPlusTree tree = indexTrees[i];

            if (tree != null) {
                ids[i] = indexTrees[i].getRootPageId();
            }
            else {
                ids[i] = -1;
            }
        }

        return ids;
    }

    public List<Integer> getPageIds() { return pageIds; }

    @Override public String  name() { return name; }

    @Override public Schema  schema() { return schema; }

    public boolean isTemporary() { return temporary; }

    @Override
    @Deprecated
    public void insert(Record record) throws DBException {
        insert(false, record, false);
    }

    public void insert(boolean indexing, Record record) throws DBException {
        insert(indexing, record, false);
    }

    @Deprecated
    public void insert(Record record, boolean allowDup) throws DBException {
        insert(false, record, allowDup);
    }

    public void insert(boolean indexing, Record record, boolean allowDup) throws DBException {
        if (storage == null || buffer == null)
            throw new DBException("Table not bound to storage/buffer");

        if (indexing && index == null) {
            try { buildIndex(storage.getPageSize() / 10); }
            catch (DBException ignored) {}
        }

        schema.validate(record);

        List<Attribute> tableAttributes = schema.getAttributes();
        List<Value> recordAttributes = record.getAttributes();

        Attribute pk = schema.getPrimaryKey();
        if (pk == null && !allowDup)
            throw new DBException("Table has no primary key");

        int    pkIndex = schema.getAttributeIndex(pk.getName());
        Object pkValue = record.getAttributes().get(pkIndex).getRaw();

        // duplicate check if indexing (check while inserting if not indexing)
        if (!allowDup) {
            if (index != null) {
                @SuppressWarnings("unchecked")
                int existingPid = index.search((Comparable<Object>) pkValue);
                if (existingPid != -1)
                    throw new DBException("duplicate primary key value: ( " + pkValue + " )");

                //Check unique values
                for (int i = 0; i < tableAttributes.size(); i++) {

                    Attribute attribute = tableAttributes.get(i);
                    Object value = recordAttributes.get(i).getRaw();
                    BPlusTree tree = indexTrees[i];

                    if (attribute.isUnique() && !attribute.isPrimaryKey()) {
                        @SuppressWarnings("unchecked")
                        int duplicatePid = tree.search((Comparable<Object>) value);
                        if(duplicatePid != -1) {  //there's another identical value
                            throw new DBException("duplicate unique value: ( " + value + " )");
                        }
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

            //Update index trees
            for (int i = 0; i < tableAttributes.size(); i++) {

                Attribute attribute = tableAttributes.get(i);
                if (attribute.isUnique() || attribute.isPrimaryKey()) {
                    updateIndex(recordAttributes.get(i).getRaw(), pid, i);
                }

            }
            return;
        }

        int targetPid = -1;
        // Use index to find the right page
        if (index != null) {
            targetPid = index.findPage((Comparable<Object>) pkValue);
        }
        // If not indexing, check all pages
        else {
            // check duplicates + find insertion page
            for (int i = 0; i < pageIds.size() - 1; i++) {
                int pid = pageIds.get(i);
                Page p = buffer.getPage(pid);
                List<Record> records = p.getRecords();

                for (Record existing : records) {
                    Object existingPk = existing.getAttributes().get(pkIndex).getRaw();
                    if (pkValue != null && pkValue.equals(existingPk) && !allowDup) {
                        throw new DBException("duplicate primary key value: ( " + pkValue + " )");
                    }
                }

                boolean isLastPage = (i == pageIds.size() - 1);

                // record belongs in this page if its key <= last key on page, OR this is the last page
                Object lastPk = records.isEmpty() ? null : records.get(records.size() - 1).getAttributes().get(pkIndex).getRaw();
                if (records.isEmpty() || compareKeys(pkValue, lastPk) <= 0 || isLastPage) {
                    targetPid = pid;
                    break;
                }
            }
        }

        //if that didn't get a valid page, go to the last page
        if (targetPid == -1) {
            int  lastIdx = pageIds.size() - 1;
            targetPid = pageIds.get(lastIdx);
        }

        //find the index of the page
        int targetIndex = -1;   //should always be changed in the for loop
        for (int i = 0; i < pageIds.size(); i++) {
            if (pageIds.get(i) == targetPid) {
                targetIndex = i;
            }
        }

        Page p       = buffer.getPage(targetPid);

        if (buffer.canFitRecord(p, record)) {
            insertIntoSortedPosition(p.getRecords(), record, pkIndex);
            buffer.markDirty(targetPid);
            //Update index trees
            for (int i = 0; i < tableAttributes.size(); i++) {
                Attribute attribute = tableAttributes.get(i);
                if (attribute.isUnique() || attribute.isPrimaryKey()) {
                    updateIndex(recordAttributes.get(i).getRaw(), targetPid, i);
                }
            }
        } else {
            insertIntoSortedPosition(p.getRecords(), record, pkIndex);
            splitPage(targetIndex, p);
            buffer.markDirty(targetPid);
            //rebuild indexes after splitting
            if (index != null) {
                rebuildIndexForPage(pageIds.get(targetIndex));
                rebuildIndexForPage(pageIds.get(targetIndex + 1));
                indexRootPageId = index.getRootPageId();
            }
        }
    }

    /**
     * Update the index on the primary ket
     * @param pkValue the value to insert or update
     * @param dataPageId the id of the page that pkValue is on
     */
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

    /**
     * Update the index on an arbitrary attribute.
     * @param value the value to insert or update
     * @param dataPageId the id of the page that value is on
     * @param attrIndex the index of the attribute whose index should be updated
     */
    private void updateIndex(Object value, int dataPageId, int attrIndex) throws DBException {
        BPlusTree tree = indexTrees[attrIndex];
        if (tree == null) return;

        @SuppressWarnings("unchecked")
        Comparable<Object> key = (Comparable<Object>) value;
        tree.delete(key);               // remove stale entry if present
        tree.insert(key, dataPageId);

        //update the root page id if the attribute was the primary key
        Attribute pk = schema.getPrimaryKey();
        if (pk != null && attrIndex == schema.getAttributeIndex(pk.getName())) {
            indexRootPageId = tree.getRootPageId();
        }

        // DEBUG: verify immediately after insert
        @SuppressWarnings("unchecked")
        int verify = tree.search((Comparable<Object>) value);
        if (verify == -1) {
            System.out.println("DEBUG updateIndex: FAILED to find key=" + value + " after insert");
        }
    }

    /**
     * Rebuild the index on the primary key for a page
     * @param pid the page
     * @param pkIndex the index of the primarykey
     */
    @Deprecated
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

    /**
     * Rebuild all of the index trees on a page
     * @param pid the page id
     */
    private void rebuildIndexForPage(int pid) throws DBException {
        Page         p       = buffer.getPage(pid);
        List<Record> records = p.getRecords();
        for (int i = 0; i < indexTrees.length; i++) {

            BPlusTree tree = indexTrees[i];

            if (tree != null) {
                for (Record r : records) {
                    Object raw = r.getAttributes().get(i).getRaw();
                    @SuppressWarnings("unchecked")
                    Comparable<Object> key = (Comparable<Object>) raw;

                    tree.delete(key);
                    tree.insert(key, pid);
                }
            }
        }
        indexRootPageId = index.getRootPageId();
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