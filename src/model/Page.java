package model;

import java.util.ArrayList;

public class Page {
    private ArrayList<Record> records;
    private int pageID;

    public Page(int id){
        records = new ArrayList<Record>();
        pageID = id;
    }

    public void addRecord(Record r){
        records.add(r);
    }

    public void addRecordAt(int index, Record r) {
        records.add(index, r);
    }

    public Record removeRecordAt(int index) {
        return records.remove(index);
    }

    public ArrayList<Record> getRecords(){
        return records;
    }

    public int size(){
        return records.size();
    }
    public int getPageID() {
        return pageID;
    }
}
