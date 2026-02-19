package catalog;

import model.Table;
import util.DBException;
import model.Schema;

public interface CatalogIN {

    void load() throws DBException;

    void createTable(String Name, Schema schema) throws DBException;

    void dropTable(String tableName) throws DBException;

    Table getTable(String tableName) throws DBException;

    boolean hasTable(String tableName);
}
