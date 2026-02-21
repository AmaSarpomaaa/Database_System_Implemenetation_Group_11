package catalog;

import model.Table;
import util.DBException;

import java.util.Map;

public interface Catalog {

    void load() throws DBException;

    void save() throws DBException;

    void addTable(Table table) throws DBException;

    void removeTable(String tableName) throws DBException;

    Table getTable(String tableName) throws DBException;

    boolean exists(String tableName);

    Map<String, Table> getTables();
}
