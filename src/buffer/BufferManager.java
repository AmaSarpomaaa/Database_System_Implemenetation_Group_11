package buffer;

import model.Page;
import model.Record;
import model.Value;
import storage.StorageManager;
import util.DBException;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

public class BufferManager{
    private int maxBufferSize;
    private int pageSize;
    private StorageManager storage;
    private Map<Integer, Page> bufferPool;
    private LinkedList<Integer> lruTracker;
    private Set<Integer> dirtyPages;

    /**
     *  Creates a new instance of a Buffer Manager
     * @param capacity the max buffer size intended
     * @param pageSize provided size of Page object
     * @param storage instance of StorageManager
     */
    public void initialize(int capacity, int pageSize, StorageManager storage){
        this.maxBufferSize = capacity;
        this.pageSize = pageSize;
        this.storage = storage;
        this.bufferPool = new HashMap<>();
        this.lruTracker = new LinkedList<>();
        this.dirtyPages = new HashSet<>();
    }

    /**
     * Retrieves a page from the buffer. If the page is not currently located within the buffer,
     * it looks through the Storage Manager. Also evicts a page if the buffer is at max.
     * @param pageId The unique identifier of a Page
     * @return The requested Page object associated with given pageId
     */
    public Page getPage(int pageId) throws DBException{
        // Check if data is already in RAM
        if(bufferPool.containsKey(pageId)){
            updateLRU(pageId); // Marks data as recently used to avoid eviction
            return bufferPool.get(pageId);
        }
        // Page not in buffer, so will need to be added to buffer
        // Make sure it fits within buffer
        if(bufferPool.size() >= maxBufferSize){
            evictIfNeeded();
        }
        // Locate Page from Storage Manager and add to buffer pool
        byte[] data = storage.readPageBytes(pageId);

        Page newPage = deserializePage(pageId, data);
        bufferPool.put(pageId, newPage);
        // add to beginning of lru tracker, making it the most recently used/accessed
        lruTracker.addFirst(pageId);
        return newPage;
    }

    /**
     * Helper function to move a pageID that already exists within the buffer to the front
     * @param pageId Unique Page object identifier
     */
    public void updateLRU(int pageId){
        // Remove from current location within buffer pool and make it most recently used
        lruTracker.remove((Integer) pageId);
        lruTracker.addFirst(pageId);
    }


    /**
     * Marks a page as modified/dirty within the buffer's dirty pages collection
     * @param pageId int
     * @throws DBException aa
     */
    public void markDirty(int pageId) throws DBException{
        if(bufferPool.containsKey(pageId)){
            dirtyPages.add(pageId);
        }
    }

    /**
     * Clears all collections
     * @throws DBException don't know
     */
    public void flushAll() throws DBException{
        for(Integer pageId: bufferPool.keySet()){
            // Iterate through dirty pages to save all modified
            if(dirtyPages.contains(pageId)){
                Page p = bufferPool.get(pageId);
                byte[] data = serializePage(p);
                storage.writePageBytes(pageId, data);
            }
        }
        // Clear all data structures used within the buffer pool
        dirtyPages.clear();
        bufferPool.clear();
        lruTracker.clear();
    }

    /**
     * Removes the last recently used element within the lruTracker
     * Checks if it is within the dirty pages as well
     * @throws DBException again dk why this would throw exception
     */
    public void evictIfNeeded() throws DBException{
        if (bufferPool.size() >= maxBufferSize){
            // Get rid of the oldest used elem
            int targetId = lruTracker.removeLast();
            Page targetPage = bufferPool.get(targetId);
            // Check if it is modified or dirty
            if (dirtyPages.contains(targetId)){
                byte[] data = serializePage(targetPage);
                storage.writePageBytes(targetId, data);
                dirtyPages.remove(targetId);
            }
            // Remove from the buffer pool
            bufferPool.remove(targetId);
        }
    }

    /**
     * Converts a Page object into a corresponding byte[]
     * @param page provided Page
     * @return byte[] representation of a Page
     */
    private byte[] serializePage(Page page){
        byte[] data = new byte[pageSize];
        ByteBuffer bufferz = ByteBuffer.wrap(data);

        List<Record> records = page.getRecords();
        int numRecords = records.size();

        bufferz.putInt(0, numRecords);
        int currentOffset = pageSize;
        // Iterate through number of Records in Page, converting to bytes, and placing them within the bytebuffer
        for (int i = 0; i < numRecords; i++){
            Record rec = records.get(i);
            byte[] recBytes = serializeRecord(rec);
            currentOffset = currentOffset - recBytes.length;

            // Calculation of offset serialize
            for(int j = 0; j < recBytes.length; j++){
                data[currentOffset + j] = recBytes[j];
            }
            int headerPos = 4 + (i * 4);
            bufferz.putInt(headerPos, currentOffset);
        }

        return data;
    }

    /**
     *  Converts a byte collection into a Page object
     * @param pageId the identification number of a Page
     * @param data byte collection
     * @return Page object
     */
    private Page deserializePage(int pageId, byte[] data){
        Page page = new Page(pageId);
        ByteBuffer bufferz = ByteBuffer.wrap(data);

        int numRecords = bufferz.getInt(0);
        // Iterate through numRecords adding associated Records to Page
        for (int i = 0; i < numRecords; i++){
            // Modify header position and calculate offset
            int headerPos = 4 + (i * 4);
            int offset = bufferz.getInt(headerPos);
            Record rec = deserializeRecord(data, offset);
            page.addRecord(rec);
        }

        return page;
    }

    /**
     * Allows for the transformation of a Record object into a byte[] needed to
     * send and receive data as bytes
     * @param rec Record
     * @return byte collection representation of provided Record
     */
    private byte[] serializeRecord(Record rec){
        // Extract attributes from given record
        List<Value> attributes = rec.getAttributes();
        // counter for number of bytes needed to store complete record in a byte[]
        int totSize = 4;

        // Iterate through number of attributes
        for (int i = 0; i < attributes.size(); i++){
            Value obje = attributes.get(i);
            Object obj = obje.getRaw();
            totSize = totSize + 1; // type byte
            if (obj instanceof Integer){
                totSize = totSize + 4;
            }
            else if (obj instanceof Double){
                totSize = totSize + 8;
            }
            else if (obj instanceof String){
                String s = (String) obj;
                totSize = totSize + 4 + s.length();
            }
            else if (obj instanceof Boolean){
                totSize = totSize + 1;
            }
            // null: just the type byte (already counted above)
        }
        // Create a byte[] with the calculated total size of attributes
        byte[] recData = new byte[totSize];
        ByteBuffer bufferz = ByteBuffer.wrap(recData);
        bufferz.putInt(attributes.size());
        // Iterate through all attributes, placing each into the empty byteBuffer
        for (int i = 0; i < attributes.size(); i++){
            Value obje = attributes.get(i);
            Object obj = obje.getRaw();

            if (obj instanceof Integer){
                bufferz.put((byte) 1);
                bufferz.putInt((Integer) obj);
            }
            else if (obj instanceof Double){
                bufferz.put((byte) 2);
                bufferz.putDouble((Double) obj);
            }
            else if (obj instanceof String){
                bufferz.put((byte) 3);
                String st = (String) obj;
                bufferz.putInt(st.length());
                for (int j = 0; j < st.length(); j++){
                    bufferz.put((byte) st.charAt(j));
                }
            }
            else if (obj instanceof Boolean){
                bufferz.put((byte) 4);
                bufferz.put((byte) (((Boolean) obj) ? 1 : 0));
            }
            else {
                // null
                bufferz.put((byte) 0);
            }
        }
        return recData;
    }

    /**
     * Converts a collection of bytes into a Record object using a given offset
     * @param data byte collection
     * @param offset int
     * @return Record object
     */
    private Record deserializeRecord(byte[] data, int offset){
        Record rec = new Record();
        ByteBuffer bufferz = ByteBuffer.wrap(data);
        bufferz.position(offset);

        int numAttributes = bufferz.getInt();

        // Iterate through number of attributes
        for (int i = 0; i < numAttributes; i++){
            // The byte buffer contains a number indicating the type of the attribute.
            byte type = bufferz.get();

            if (type == 0){
                rec.addAttribute(new Value(null));
            }
            else if (type == 1){
                int val = bufferz.getInt();
                rec.addAttribute(new Value(val));
            }
            else if (type == 2){
                double val = bufferz.getDouble();
                rec.addAttribute(new Value(val));
            }
            else if (type == 3){
                int len = bufferz.getInt();
                byte[] strBytes = new byte[len];
                for (int j = 0; j < len; j++){
                    strBytes[j] = bufferz.get();
                }
                String val = new String(strBytes);
                rec.addAttribute(new Value(val));
            }
            else if (type == 4){
                boolean val = bufferz.get() == 1;
                rec.addAttribute(new Value(val));
            }
        }
        return rec;
    }
}