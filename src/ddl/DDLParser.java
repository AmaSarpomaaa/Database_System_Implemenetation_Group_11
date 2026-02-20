package ddl;

import catalog.Catalog;
import model.*;
import util.DBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DDLParser implements DDLProcessor {

    private final Catalog catalog;

    public DDLParser(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Result createTable(ParsedCommand cmd) throws DBException {
        if (!(cmd instanceof CreateTableCommand)) {
            throw new DBException("Internal error: expected CreateTableCommand.");
        }

        CreateTableCommand c = (CreateTableCommand) cmd;
        String tableName = c.getTableName();

        if (catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' already exists.");
        }

        List<Attribute> attrs = Arrays.asList(c.getAttributes());

        // exactly one PRIMARYKEY
        int pkCount = 0;
        for (Attribute a : attrs) {
            if (a.isPrimaryKey()) pkCount++;
        }
        if (pkCount != 1) {
            throw new DBException("Table must have exactly one PRIMARYKEY attribute.");
        }

        Schema schema = new Schema(attrs);
        TableSchema table = new TableSchema(tableName, schema);

        catalog.addTable(table);

        return Result.ok("Table created successfully");
    }

    @Override
    public Result dropTable(ParsedCommand cmd) throws DBException {
        if (!(cmd instanceof DropTableCommand)) {
            throw new DBException("Internal error: expected DropTableCommand.");
        }

        DropTableCommand d = (DropTableCommand) cmd;
        String tableName = d.getTableName();

        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        catalog.removeTable(tableName);

        return Result.ok("Table dropped successfully");
    }

    @Override
    public Result alterTableAdd(ParsedCommand cmd) throws DBException {
        if (!(cmd instanceof AlterTableAddCommand)) {
            throw new DBException("Internal error: expected AlterTableAddCommand.");
        }

        AlterTableAddCommand a = (AlterTableAddCommand) cmd;
        String tableName = a.getTableName();
        Attribute newAttr = a.getAttribute();

        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        Table oldT = catalog.getTable(tableName);
        Schema oldS = oldT.schema();

        // already exists?
        if (oldS.hasAttribute(newAttr.getName())) {
            throw new DBException("Attribute '" + newAttr.getName()
                    + "' already exists in table '" + tableName + "'.");
        }

        // NOTNULL requires DEFAULT
        if (a.isNotNull() && !a.hasDefaultValue()) {
            throw new DBException("Error: Not null requires a default value when altering a table");
        }

        // build new schema (append)
        List<Attribute> newAttrs = new ArrayList<>(oldS.getAttributes());
        newAttrs.add(newAttr);
        Schema newSchema = new Schema(newAttrs);

        TableSchema newTable = new TableSchema(tableName, newSchema);

        Object defaultVal = a.hasDefaultValue() ? a.getDefaultValue() : null;

        // copy records, append default
        for (model.Record rOld : oldT.scan()) {
            model.Record rNew = new model.Record();
            for (Object v : rOld.getAttributes()) {
                rNew.addAttribute(v);
            }
            rNew.addAttribute(defaultVal);
            newTable.insert(rNew);
        }

        catalog.removeTable(tableName);
        catalog.addTable(newTable);

        return Result.ok("Table altered successfully");
    }

    @Override
    public Result alterTableDrop(ParsedCommand cmd) throws DBException {
        if (!(cmd instanceof AlterTableDropCommand)) {
            throw new DBException("Internal error: expected AlterTableDropCommand.");
        }

        AlterTableDropCommand d = (AlterTableDropCommand) cmd;
        String tableName = d.getTableName();
        String attrName = d.getAttributeName();

        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        Table oldT = catalog.getTable(tableName);
        Schema oldS = oldT.schema();

        if (!oldS.hasAttribute(attrName)) {
            throw new DBException("Attribute '" + attrName
                    + "' does not exist in table '" + tableName + "'.");
        }

        // cannot drop primary key
        Attribute pk = oldS.getPrimaryKey();
        if (pk != null && pk.getName().equalsIgnoreCase(attrName)) {
            throw new DBException("Error: Cannot drop primary key attribute");
        }

        int dropIndex = oldS.getAttributeIndex(attrName);

        // new schema without dropped attr
        List<Attribute> oldAttrs = oldS.getAttributes();
        List<Attribute> newAttrs = new ArrayList<>();
        for (int i = 0; i < oldAttrs.size(); i++) {
            if (i != dropIndex) newAttrs.add(oldAttrs.get(i));
        }
        Schema newSchema = new Schema(newAttrs);

        TableSchema newTable = new TableSchema(tableName, newSchema);

        // copy records skipping dropped value
        for (model.Record rOld : oldT.scan()) {
            model.Record rNew = new model.Record();
            List<Value> vals = rOld.getAttributes();
            for (int i = 0; i < vals.size(); i++) {
                if (i != dropIndex) rNew.addAttribute(vals.get(i));
            }
            newTable.insert(rNew);
        }

        catalog.removeTable(tableName);
        catalog.addTable(newTable);

        return Result.ok("Table altered successfully");
    }
}