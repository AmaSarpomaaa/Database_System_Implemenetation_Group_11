package model;

import util.DBException;
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
        Page newPage = new Page(1000, pages.size());
        newPage.addRecord(record);
        pages.addLast(newPage);
    }

    @Override
    public List<Record> scan() throws DBException {
        List<Record> result = new ArrayList<>();
        for (Page page : pages) {
            result.addAll(page.getRecords());
        }
        return result;
    }
}