package buffer;

import model.Page;
import storage.StorageManager;
import util.DBException;

public interface BufferManager {

    void initialize(int capacity,
                    int pageSize,
                    StorageManager storage) throws DBException;

    Page getPage(int pageId) throws DBException;

    void markDirty(int pageId) throws DBException;

    void flushAll() throws DBException;

    void evictIfNeeded() throws DBException;
}
