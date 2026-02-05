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

    public ArrayList<Record> getRecords(){
        return records;
    }

    public int getPageID() {
        return pageID;
    }
}
