package storage;

import util.DBException;

import java.util.Arrays;

public class StorageTest {

    public static void main(String[] args) throws DBException {

        String file = "mydatabase.db";

        StorageManager sm = new FileStorageManager();

        // Open database
        sm.open(file, 4096);

        System.out.println("Page size = " + sm.getPageSize());

        // Allocate a new page
        int pageId = sm.allocatePage();
        System.out.println("Allocated page = " + pageId);

        // Write some bytes
        byte[] data = new byte[sm.getPageSize()];
        data[0] = 10;
        data[1] = 20;

        sm.writePageBytes(pageId, data);
        sm.close();

        // Reopen to test persistence
        StorageManager sm2 = new FileStorageManager();
        sm2.open(file, 9999); // should ignore and use stored page size

        byte[] read = sm2.readPageBytes(pageId);

        System.out.println("After restart page size = " + sm2.getPageSize());
        System.out.println("Read bytes = " + read[0] + ", " + read[1]);
        System.out.println("Match = " + Arrays.equals(data, read));

        sm2.close();
    }
}
