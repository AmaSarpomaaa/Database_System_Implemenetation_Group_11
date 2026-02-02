package storage;

import util.DBException;

public interface StorageManager {

    void open(String path, int pageSize) throws DBException;

    void close() throws DBException;

    byte[] readPageBytes(int pageId) throws DBException;

    void writePageBytes(int pageId, byte[] data) throws DBException;

    int allocatePage() throws DBException;

    void freePage(int pageId) throws DBException;

    int getPageSize();
}
