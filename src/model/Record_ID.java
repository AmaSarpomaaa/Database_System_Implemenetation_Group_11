package model;

/**
 * RID = Record ID
 * Tells where a record is stored on disk.
 */
public class Record_ID {

    private int pageId;
    private int slotId;

    public Record_ID(int pageId, int slotId) {
        this.pageId = pageId;
        this.slotId = slotId;
    }

    public int getPageId() {
        return pageId;
    }

    public int getSlotId() {
        return slotId;
    }

    @Override
    public String toString() {
        return "RID(" + pageId + "," + slotId + ")";
    }
}
