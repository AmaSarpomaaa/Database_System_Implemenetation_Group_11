package buffer;

import model.Page;
import model.Record;
import storage.StorageManager;
import util.DBException;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

public class BufferManager {
    private int maxBufferSize;
    private int pageSize;
    private StorageManager storage;
    private Map<Integer, Page> bufferPool;
    private LinkedList<Integer> lruTracker;
    private Set<Integer> dirtyPages;

    /**
     *
     * @param capacity
     * @param pageSize
     * @param storage
     */
    public void initialize(int capacity, int pageSize, StorageManager storage) {
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
    public Page getPage(int pageId) throws DBException {
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
     * @param pageId
     * @throws DBException
     */
    public void markDirty(int pageId) throws DBException {
        // Look for Page associated within bufferPool. Add to dirtyPages
        if(bufferPool.containsKey(pageId)){
            dirtyPages.add(pageId);
        }
    }

    /**
     * Clears all collections
     * @throws DBException
     */
    public void flushAll() throws DBException {
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
     * @throws DBException
     */
    public void evictIfNeeded() throws DBException {
        if (bufferPool.size() >= maxBufferSize) {
            // Get rid of the oldest used elem
            int targetId = lruTracker.removeLast();
            Page targetPage = bufferPool.get(targetId);
            // Check if it is modified or dirty
            if (dirtyPages.contains(targetId)) {
                byte[] data = serializePage(targetPage);
                storage.writePageBytes(targetId, data);
                dirtyPages.remove(targetId);
            }
            // Remove from the buffer pool
            bufferPool.remove(targetId);
        }
    }

    private byte[] serializePage(Page page) {
        byte[] data = new byte[pageSize];
        ByteBuffer buffer = ByteBuffer.wrap(data);

        List<Record> records = page.getRecords();
        int numRecords = records.size();

        buffer.putInt(0, numRecords);

        int currentOffset = pageSize;

        for (int i = 0; i < numRecords; i++) {
            Record rec = records.get(i);
            byte[] recBytes = serializeRecord(rec);

            currentOffset = currentOffset - recBytes.length;

            for(int j = 0; j < recBytes.length; j++) {
                data[currentOffset + j] = recBytes[j];
            }

            int headerPos = 4 + (i * 4);
            buffer.putInt(headerPos, currentOffset);
        }

        return data;
    }

    private Page deserializePage(int pageId, byte[] data) {
        Page page = new Page(pageId);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int numRecords = buffer.getInt(0);

        for (int i = 0; i < numRecords; i++) {
            int headerPos = 4 + (i * 4);
            int offset = buffer.getInt(headerPos);

            Record rec = deserializeRecord(data, offset);
            page.addRecord(rec);
        }

        return page;
    }

    private byte[] serializeRecord(Record rec) {
        return new byte[0];
    }

    private Record deserializeRecord(byte[] data, int offset) {
        return new Record();
    }
}