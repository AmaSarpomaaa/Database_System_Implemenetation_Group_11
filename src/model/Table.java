package model;

import util.DBException;
import java.util.List;

public interface Table {

    String name();
    Schema schema();
    boolean isTemporary();
    void insert(Record record) throws DBException;

    List<Record> scan() throws DBException;

    List<Integer> getPageIds() throws DBException;
}
