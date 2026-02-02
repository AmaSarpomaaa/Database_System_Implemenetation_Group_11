package index;

import model.RID;
import model.Value;
import util.DBException;

import java.util.List;

public interface Index {

    void insert(Value key, RID rid) throws DBException;

    void delete(Value key, RID rid) throws DBException;

    List<RID> search(Value key) throws DBException;
}
