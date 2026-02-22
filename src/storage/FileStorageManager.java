package storage;

import util.DBException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class FileStorageManager implements StorageManager {

    private static final byte[] MAGIC = "JOTTQL1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_FIXED_SIZE = MAGIC.length + 4 + 4; // 15 bytes: magic + version + pageSize

    private RandomAccessFile raf;
    private int pageSize;
    private final java.util.Deque<Integer> freeList = new java.util.ArrayDeque<>();

    @Override
    public void open(String path, int providedPageSize) throws DBException {
        try {
            File file = new File(path);
            boolean newDb = (!file.exists()) || file.length() == 0;

            raf = new RandomAccessFile(file, "rw");

            if (newDb || raf.length() == 0) {
                this.pageSize = providedPageSize;
                writeHeaderPage0();
            } else {
                readHeaderPage0(); // sets this.pageSize from file
            }
        } catch (IOException e) {
            throw new DBException("Failed to open database file: " + path, e);
        }
    }

    @Override
    public void close() throws DBException {
        try {
            writeHeaderPage0(); // persist free list on close
            if (raf != null) raf.close();
        } catch (IOException e) {
            throw new DBException("Failed to close database file", e);
        }
    }

    @Override
    public byte[] readPageBytes(int pageId) throws DBException {
        try {
            if (pageId < 0) throw new DBException("Invalid pageId: " + pageId);

            long offset = (long) pageId * pageSize;
            long end = offset + pageSize;

            if (end > raf.length()) {
                throw new DBException("Page out of bounds: " + pageId);
            }

            raf.seek(offset);
            byte[] data = new byte[pageSize];
            raf.readFully(data);
            return data;

        } catch (IOException e) {
            throw new DBException("Failed to read page " + pageId, e);
        }
    }

    @Override
    public void writePageBytes(int pageId, byte[] data) throws DBException {
        if (data == null || data.length != pageSize) {
            throw new DBException("writePageBytes requires byte[] length == pageSize (" + pageSize + ")");
        }

        try {
            if (pageId < 0) throw new DBException("Invalid pageId: " + pageId);

            long offset = (long) pageId * pageSize;
            raf.seek(offset);
            raf.write(data);

        } catch (IOException e) {
            throw new DBException("Failed to write page " + pageId, e);
        }
    }

    @Override
    public int allocatePage() throws DBException {
        // Reuse a freed page if available
        if (!freeList.isEmpty()) {
            int reused = freeList.pop();
            writeHeaderPage0(); // update persisted free list
            return reused;
        }

        try {
            long length = raf.length();

            // Ensure header exists (page 0)
            if (length < pageSize) {
                raf.setLength(pageSize);
                length = raf.length();
            }

            int newPageId = (int) (length / pageSize);

            // Grow file by one full page
            raf.setLength(length + pageSize);

            return newPageId;

        } catch (IOException e) {
            throw new DBException("Failed to allocate page", e);
        }
    }

    @Override
    public void freePage(int pageId) throws DBException {
        if (pageId <= 0) return;
        freeList.push(pageId);
        writeHeaderPage0();
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    // -------- Header Page 0 helpers --------

    private void writeHeaderPage0() throws DBException {
        try {
            ByteBuffer buf = ByteBuffer.allocate(pageSize);

            buf.put(MAGIC);        // 7 bytes
            buf.putInt(VERSION);   // 4 bytes
            buf.putInt(pageSize);  // 4 bytes

            int maxEntries = (pageSize - HEADER_FIXED_SIZE - 4) / 4; // -4 for freeCount field
            int toWrite = Math.min(freeList.size(), maxEntries);
            buf.putInt(toWrite);
            int written = 0;
            for (int id : freeList) {
                if (written++ >= toWrite) break;
                buf.putInt(id);
            }

            raf.seek(0);
            raf.write(buf.array());

            if (raf.length() < pageSize) {
                raf.setLength(pageSize);
            }
        } catch (IOException e) {
            throw new DBException("Failed to write header page 0", e);
        }
    }

    private void readHeaderPage0() throws DBException {
        try {
            raf.seek(0);
            byte[] fixedData = new byte[HEADER_FIXED_SIZE];
            raf.readFully(fixedData);
            ByteBuffer fixedBuf = ByteBuffer.wrap(fixedData);

            // Validate magic
            byte[] magic = new byte[MAGIC.length];
            fixedBuf.get(magic);
            for (int i = 0; i < MAGIC.length; i++) {
                if (magic[i] != MAGIC[i]) {
                    throw new DBException("Not a valid JottQL database file (bad magic).");
                }
            }

            int version = fixedBuf.getInt();
            if (version != VERSION) {
                throw new DBException("Unsupported database version: " + version);
            }

            this.pageSize = fixedBuf.getInt();
            if (pageSize <= 0) throw new DBException("Corrupt header: invalid pageSize " + pageSize);

            raf.seek(0);
            byte[] data = new byte[pageSize];
            raf.readFully(data);
            ByteBuffer buf = ByteBuffer.wrap(data);
            buf.position(HEADER_FIXED_SIZE);

            // Load free list
            freeList.clear();
            int freeCount = buf.getInt();
            for (int i = 0; i < freeCount; i++) {
                freeList.push(buf.getInt());
            }

        } catch (IOException e) {
            throw new DBException("Failed to read header page 0", e);
        }
    }
}