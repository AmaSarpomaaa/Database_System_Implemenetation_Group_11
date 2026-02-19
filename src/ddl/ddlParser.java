package DDL;

import model.*;
import parser.ParsedCommand;
import parser.CommandType;
import util.DBException;
import java.util.ArrayList;
import java.util.List;

public class ddlParser implements DDLProcessor {

    private final Catalog catalog;

    public ddlParser(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public Result createTable(ParsedCommand cmd) throws DBException {
        String tableName = cmd.getTableNames().get(0);

        // Check if table already exists
        if (catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' already exists.");
        }

        // Build attributes from parsed command
        // Assumption: attributes = ["x1", "x2"] values = ["INTEGER PRIMARYKEY", "DOUBLE NOTNULL", ...]
        List<Attribute> attributes = parseAttributes(cmd.getAttributes(), cmd.getValues());

        // Validate exactly one primary key exists
        int primaryKeyCount = 0;
        for (Attribute attr : attributes) {
            if (attr.unique) {
                primaryKeyCount++;
            }
        }
        if (primaryKeyCount != 1) {
            throw new DBException("Table must have exactly one PRIMARYKEY attribute.");
        }

        // Create schema and table
        Schema schema = new Schema(attributes);
        TableSchema table = new TableSchema(tableName, schema);
        catalog.addTable(tableName, table);

        return Result.ok("Table '" + tableName + "' created successfully.");
    }

    @Override
    public Result dropTable(ParsedCommand cmd) throws DBException {
        String tableName = cmd.getTableNames().get(0);

        // Check if table exists
        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        // Remove from catalog
        catalog.removeTable(tableName);

        return Result.ok("Table '" + tableName + "' dropped successfully.");
    }

    @Override
    public Result alterTableAdd(ParsedCommand cmd) throws DBException {
        String tableName = cmd.getTableNames().get(0);

        // Check if table exists
        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        TableSchema oldTable = catalog.getTable(tableName);

        // Parse new attribute from command
        // Assumption: attributes = ["x2"] values = ["INTEGER", "NOTNULL", "DEFAULT", 5] etc
        String attrName = cmd.getAttributes().get(0);
        Attribute newAttr = parseAttribute(attrName, cmd.getValues());
        Object defaultValue = extractDefaultValue(cmd.getValues());

        // Validate attribute doesn't already exist
        if (oldTable.schema().hasAttribute(attrName)) {
            throw new DBException("Attribute '" + attrName + "' already exists in table '" + tableName + "'.");
        }

        // Validate NOTNULL constraint has a default value
        if (newAttr.not_null && defaultValue == null) {
            throw new DBException("NOTNULL attribute '" + attrName + "' requires a DEFAULT value.");
        }

        // Create new schema with added attribute
        List<Attribute> newAttributes = new ArrayList<>(oldTable.schema().attributeList);
        newAttributes.add(newAttr);
        Schema newSchema = new Schema(newAttributes);

        // Create temporary table with new schema
        TableSchema tempTable = new TableSchema(tableName, newSchema);

        // Copy all records from old table, appending default value
        for (Record oldRecord : oldTable.scan()) {
            Record newRecord = new Record();
            for (Object value : oldRecord.getAttributes()) {
                newRecord.addAttribute(value);
            }
            newRecord.addAttribute(defaultValue);
            tempTable.insert(newRecord);
        }

        // Replace old table with new table
        catalog.removeTable(tableName);
        catalog.addTable(tableName, tempTable);

        return Result.ok("Attribute '" + attrName + "' added to table '" + tableName + "'.");
    }

    @Override
    public Result alterTableDrop(ParsedCommand cmd) throws DBException {
        String tableName = cmd.getTableNames().get(0);
        String attrName = cmd.getAttributes().get(0);

        // Check if table exists
        if (!catalog.exists(tableName)) {
            throw new DBException("Table '" + tableName + "' does not exist.");
        }

        TableSchema oldTable = catalog.getTable(tableName);

        // Validate attribute exists
        if (!oldTable.schema().hasAttribute(attrName)) {
            throw new DBException("Attribute '" + attrName + "' does not exist in table '" + tableName + "'.");
        }

        // Prevent dropping primary key
        int dropIndex = oldTable.schema().getAttributeIndex(attrName);
        if (oldTable.schema().attributeList.get(dropIndex).unique) {
            throw new DBException("Cannot drop primary key attribute '" + attrName + "'.");
        }

        // Create new schema without the dropped attribute
        List<Attribute> newAttributes = new ArrayList<>();
        for (int i = 0; i < oldTable.schema().attributeList.size(); i++) {
            if (i != dropIndex) {
                newAttributes.add(oldTable.schema().attributeList.get(i));
            }
        }
        Schema newSchema = new Schema(newAttributes);

        // Create temporary table with new schema
        TableSchema tempTable = new TableSchema(tableName, newSchema);

        // Copy all records from old table, skipping dropped attribute
        for (Record oldRecord : oldTable.scan()) {
            Record newRecord = new Record();
            for (int i = 0; i < oldRecord.getAttributes().size(); i++) {
                if (i != dropIndex) {
                    newRecord.addAttribute(oldRecord.getAttributes().get(i));
                }
            }
            tempTable.insert(newRecord);
        }

        // Replace old table with new table
        catalog.removeTable(tableName);
        catalog.addTable(tableName, tempTable);

        return Result.ok("Attribute '" + attrName + "' dropped from table '" + tableName + "'.");
    }

    // Helper methods for parsing attributes from ParsedCommand

    /**
     * Parse a list of attributes for CREATE TABLE
     * Assumption:
     *   attributes = ["x1", "x2", "x3"]
     *   values = ["INTEGER PRIMARYKEY", "DOUBLE NOTNULL", "VARCHAR(10)"]
     */
    private List<Attribute> parseAttributes(List<String> attrNames, List<Object> attrSpecs) throws DBException {
        List<Attribute> result = new ArrayList<>();

        for (int i = 0; i < attrNames.size(); i++) {
            String name = attrNames.get(i);
            String spec = (String) attrSpecs.get(i);
            result.add(parseAttribute(name, spec));
        }

        return result;
    }

    /**
     * Parse a single attribute specification string.
     *
     * Format: "<datatype> [PRIMARYKEY] [NOTNULL]"
     * Examples: "INTEGER PRIMARYKEY", "DOUBLE NOTNULL", "CHAR(5)"
     */
    private Attribute parseAttribute(String name, String spec) throws DBException {
        String[] tokens = spec.trim().split("\\s+");

        // Parse datatype (first token)
        Datatype datatype = parseDatatype(tokens[0]);

        // Parse constraints
        boolean notNull = false;
        boolean isPrimaryKey = false;

        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i].toUpperCase();
            if (token.equals("PRIMARYKEY")) {
                isPrimaryKey = true;
                notNull = true;  // PRIMARYKEY implies NOTNULL
            } else if (token.equals("NOTNULL")) {
                notNull = true;
            }
        }

        return new Attribute(name, notNull, isPrimaryKey, datatype);
    }

    /**
     * Parse attribute for ALTER TABLE ADD
     * Assumption: values = ["INTEGER", "NOTNULL", "DEFAULT", 5]
     *             or values = ["INTEGER"]
     */
    private Attribute parseAttribute(String name, List<Object> values) throws DBException {
        // Extract datatype (first element)
        String datatypeStr = (String) values.get(0);
        Datatype datatype = parseDatatype(datatypeStr);

        // Check for constraints
        boolean notNull = false;
        for (Object val : values) {
            if (val instanceof String) {
                String str = ((String) val).toUpperCase();
                if (str.equals("NOTNULL")) {
                    notNull = true;
                }
            }
        }

        return new Attribute(name, notNull, false, datatype);
    }

    /**
     * Parse a datatype string like "INTEGER", "CHAR(5)", "VARCHAR(10)".
     */
    private Datatype parseDatatype(String str) throws DBException {
        String upper = str.toUpperCase();

        if (upper.equals("INTEGER")) {
            return Datatype.INTEGER;
        } else if (upper.equals("DOUBLE")) {
            return Datatype.DOUBLE;
        } else if (upper.equals("BOOLEAN")) {
            return Datatype.BOOLEAN;
        } else if (upper.startsWith("CHAR(")) {
            // TODO: Extract length and store in Attribute if needed
            return Datatype.CHAR;
        } else if (upper.startsWith("VARCHAR(")) {
            // TODO: Extract length and store in Attribute if needed
            return Datatype.VARCHAR;
        } else {
            throw new DBException("Unknown datatype: " + str);
        }
    }

    /**
     * Extract default value from ALTER TABLE ADD command
     * Assumption: values contains "DEFAULT" followed by the actual value.
     */
    private Object extractDefaultValue(List<Object> values) {
        for (int i = 0; i < values.size() - 1; i++) {
            if (values.get(i) instanceof String) {
                String str = ((String) values.get(i)).toUpperCase();
                if (str.equals("DEFAULT")) {
                    return values.get(i + 1);
                }
            }
        }
        return null;  // No default specified
    }
}
