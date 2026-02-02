package model;

import util.DBException;
import java.util.List;

public interface Table {

    String name();

//    Schema schema();

    void insert(Record record) throws DBException;

    List<Record> scan() throws DBException;
}
