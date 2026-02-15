package catalog;

import model.Table;
import util.DBException;

public interface CatalogIN {

    void load() throws DBException;

    void save() throws DBException;

    void addTable(Table table) throws DBException;

    void removeTable(String tableName) throws DBException;

    Table getTable(String tableName) throws DBException;

    boolean exists(String tableName);
}
