package ddl;

import model.*;
import util.DBException;

public interface DDLProcessor {
    Result createTable(CreateTableCommand cmd) throws DBException;
    Result dropTable(DropTableCommand cmd) throws DBException;
    Result alterTableAdd(AlterTableAddCommand cmd) throws DBException;
    Result alterTableDrop(AlterTableDropCommand cmd) throws DBException;
}