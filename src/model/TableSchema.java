package model;

import util.DBException;

import java.util.*;

import buffer.BufferManager;
import catalog.Catalog;

public class TableSchema implements Table {
    private final String name;
    private final Schema schema;
    private final List<Integer> pageIds;
    private final BufferManager bufferManager;
    private final Catalog catalog;
    private final Map<Object, Boolean> primaryKeyIndex;

    public TableSchema(String name, Schema schema, BufferManager bufferManager, Catalog catalog) {
        this.name = name;
        this.schema = schema;
        this.bufferManager = bufferManager;
        this.catalog = catalog;
        this.pageIds = new ArrayList<>();
        this.primaryKeyIndex = new HashMap<>();
    }

    @Override
    public String name() { return name; }

    @Override
    public Schema schema() { return schema; }

    @Override
    public void insert(Record record) throws DBException {
        schema.validate(record);

        Attribute pk = schema.getPrimaryKey();
        Object pkValue = null;
        if (pk != null) {
            int pkIndex = schema.getAttributeIndex(pk.getName());
            pkValue = record.getAttributes().get(pkIndex);
            if (pkValue != null && primaryKeyIndex.containsKey(pkValue))
                throw new DBException("Primary key violation on attribute: " + pk.getName());
        }

        // Write to last page if not full, else allocate new
        if (!pageIds.isEmpty()) {
            int lastId = pageIds.get(pageIds.size() - 1);
            Page lastPage = bufferManager.getPage(lastId);
            if (!lastPage.isFull()) {
                lastPage.insertRecord(record);
                bufferManager.markDirty(lastId);
                if (pkValue != null) primaryKeyIndex.put(pkValue, true);
                return;
            }
        }

        int newId = catalog.allocatePageId();
        pageIds.add(newId);
        Page newPage = bufferManager.getPage(newId);
        newPage.insertRecord(record);
        bufferManager.markDirty(newId);
        if (pkValue != null) primaryKeyIndex.put(pkValue, true);
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TableSchema implements Table {

    private final String name;
    private final Schema schema;
    private final LinkedList<Page> pages;

    public TableSchema(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
        this.pages = new LinkedList<Page>();
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
        // Validate types
        schema.validate(record);

        // Check primary key is unique
        Attribute pk = schema.getPrimaryKey();
        if (pk != null) {
            int pkIndex = schema.getAttributeIndex(pk.name());
            Object pkValue = record.getAttributes().get(pkIndex);
            for (Page page : pages) {
                for (Record existing : page.getRecords()) {
                    Object existingPk = existing.getAttributes().get(pkIndex);
                    if (pkValue != null && pkValue.equals(existingPk)) {
                        throw new DBException("Primary key violation on attribute: " + pk.name());
                    }
                }
            }
        }

        // One record per page until isFull() is implemented
        Page newPage = new Page(pages.size());
        newPage.addRecord(record);
        pages.addLast(newPage);
    }

    @Override
    public List<Record> scan() throws DBException {
        List<Record> result = new ArrayList<>();
        for (int pageId : pageIds) {
            Page page = bufferManager.getPage(pageId);
        for (Page page : pages) {
            result.addAll(page.getRecords());
        }
        return result;
    }
}