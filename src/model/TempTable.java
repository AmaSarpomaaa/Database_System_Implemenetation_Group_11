package model;

import util.DBException;
import java.util.ArrayList;
import java.util.List;

/**
 * A temporary, in-memory table used during query execution
 * (e.g. for Cartesian products). Never persisted to disk.
 */
public class TempTable implements Table {

    private final String name;
    private final Schema schema;
    private final List<Record> records = new ArrayList<>();

    public TempTable(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    @Override
    public String name() { return name; }

    @Override
    public Schema schema() { return schema; }

    @Override
    public List<Integer> getPageIds() {
        return new ArrayList<>();
    }

    @Override
    public void insert(Record record) throws DBException {
        schema.validate(record);
        records.add(record);
    }

    @Override
    public List<Record> scan() throws DBException {
        return new ArrayList<>(records);
    }
}
