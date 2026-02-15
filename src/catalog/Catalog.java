package catalog;

import model.Schema;
import model.Table;
import util.DBException;

import java.io.File;
import java.util.Map;

public class Catalog implements CatalogIN {
    Map<String, Table> tables;
    private final int page_size;
    public Catalog(int page_size){
        this.page_size = page_size;
    }

    public void load() throws DBException{
        
    }

    public void save() throws DBException{

    }

    public void addTable(Table table) throws DBException{

    }

    public void removeTable(String tableName) throws DBException{

    }

    public Table getTable(String tableName) throws DBException{
        return null;
    }

    public boolean exists(String tableName){
        return false;
    }
    public int getPage_size() {
        return page_size;
    }
}
