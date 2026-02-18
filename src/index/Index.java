package index;

import model.Record_ID;
import model.Value;
import util.DBException;

import java.util.List;

public interface Index {

    void insert(Value key, Record_ID rid) throws DBException;

    void delete(Value key, Record_ID rid) throws DBException;

    List<Record_ID> search(Value key) throws DBException;
}
