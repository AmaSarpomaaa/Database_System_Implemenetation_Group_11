package storage;

import util.DBException;

import java.util.Arrays;

public class StorageSmokeTest {
    public static void main(String[] args) throws DBException {
        String dbFile = "database_test.db";

        StorageManager sm = new FileStorageManager();
        sm.open(dbFile, 4096);

        int pid = sm.allocatePage(); // should be 1 (page 0 is header)
        byte[] data = new byte[sm.getPageSize()];
        data[0] = 77;
        data[1] = 88;

        sm.writePageBytes(pid, data);
        sm.close();

        StorageManager sm2 = new FileStorageManager();
        sm2.open(dbFile, 1234); // should ignore and read stored pageSize
        byte[] read = sm2.readPageBytes(pid);

        System.out.println("PageSize after restart: " + sm2.getPageSize());
        System.out.println("First two bytes: " + read[0] + ", " + read[1]);
        System.out.println("Bytes match: " + Arrays.equals(data, read));

        sm2.close();
    }
}
