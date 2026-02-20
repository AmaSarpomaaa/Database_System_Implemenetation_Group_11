package engine;

import buffer.BufferManager;
import catalog.Catalog;
import catalog.FileCatalog;
import storage.FileStorageManager;
import storage.StorageManager;
import util.DBException;

public class SimpleDBEngine implements DBEngine {

    private StorageManager storage;
    private BufferManager buffer;
    private Catalog catalog;

    @Override
    public void startup(String dbLocation, int pageSize, int bufferSize, boolean indexingEnabled) throws DBException {

        // 1) Storage Manager (disk)
        storage = new FileStorageManager();
        storage.open(dbLocation, pageSize);

        // 2) Catalog (table definitions)
        catalog = new FileCatalog(dbLocation);
        catalog.load();

        // 3) Buffer Manager (RAM cache of pages)
        buffer = new BufferManager();
        buffer.initialize(bufferSize, storage.getPageSize(), storage);

        // Phase 1: indexingEnabled is accepted but can be ignored unless your phase requires it
    }

    @Override
    public void shutdown() throws DBException {

        // Save schemas first (catalog)
        if (catalog != null) {
            catalog.save();
        }

        // Flush dirty pages to disk
        if (buffer != null) {
            buffer.flushAll();
        }

        // Close database file
        if (storage != null) {
            storage.close();
        }
    }

    // Helpful getters if your processors need them (optional but useful)
    public Catalog getCatalog() { return catalog; }
    public BufferManager getBuffer() { return buffer; }
    public StorageManager getStorage() { return storage; }
}