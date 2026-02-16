package model;

import java.util.ArrayList;
import java.util.List;

public class Page {
    private int pageId;
    private List<byte[]> records;  // Just a list of records

    public void initialize() {
        records = new ArrayList<>();
    }

    public int insertRecord(byte[] record) {
        records.add(record);
        return records.size() - 1;  // slot ID
    }

    public byte[] getRecord(int slotId) {
        return records.get(slotId);
    }

    public int getNumRecords() {
        return records.size();
    }
}
