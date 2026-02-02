package engine;

import model.Result;
import util.DBException;
import util.ParseException;

public interface DBEngine {

    void startup(String dbLocation,
                 int pageSize,
                 int bufferSize,
                 boolean indexingEnabled) throws DBException;

    Result execute(String sql) throws DBException, ParseException;

    void shutdown() throws DBException;
}
