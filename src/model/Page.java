package model;

import java.util.ArrayList;
import java.util.List;
import model.Record;

public class Page {
    private int page_Id;
    private List<Record> records;  // Just a list of records

    //TODO get size from buffer manager

    public Page(int size, int pageid) {
        records = new ArrayList<>();
        page_Id = pageid;
    }

    public void initialize() {
        records = new ArrayList<>();
    }

    public void insertRecord(Record record) {
        records.add(record);
    }

    public List<Record> getRecords() {
        return records;
    }

    public Record getRecord(int slotId) {
        return records.get(slotId);
    }

    public int getNumRecords() {
        return records.size();
    }

    public int getPageId() {
        return page_Id;
    }

    public boolean isFull(){
            return false;
    }

}
