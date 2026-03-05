package model;

import util.DBException;
import java.util.List;

public class Schema {
    List<Attribute> attributeList;

    public Schema(List<Attribute> attr) {
        attributeList = attr;
    }

    /**
     * Get the primary key attribute, or null if none exists.
     */
    public Attribute getPrimaryKey() {
        for (Attribute attr : attributeList) {
            if (attr.unique) {  // unique field represents PRIMARYKEY
                return attr;
            }
        }
        return null;
    }

    /**
     * Get the index of an attribute by name
     * Returns -1 if not found
     */
    public int getAttributeIndex(String name) {
        for (int i = 0; i < attributeList.size(); i++) {
            if (attributeList.get(i).name.equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Check if an attribute with the given name exists
     */
    public boolean hasAttribute(String name) {
        return getAttributeIndex(name) != -1;
    }

    /**
     * Validate a record against this schema.
     * Checks: correct number of values, type compatibility, NOT NULL constraints.
     */
    public void validate(Record record) throws DBException {
        List<Object> values = record.getAttributes();

        // Check arity
        if (values.size() != attributeList.size()) {
            throw new DBException("Record has " + values.size() + " values but schema expects " + attributeList.size());
        }

        // Check each attribute
        for (int i = 0; i < attributeList.size(); i++) {
            Attribute attr = attributeList.get(i);
            Object value = values.get(i);

            // Check NOT NULL constraint
            if (value == null && attr.not_null) {
                throw new DBException("Attribute '" + attr.name + "' cannot be NULL");
            }

            // Check type compatibility (only if value is non-null)
            if (value != null) {
                validateType(attr, value);
            }
        }
    }

    /**
     * Validate that a value matches the attribute's datatype.
     */
    private void validateType(Attribute attr, Object value) throws DBException {
        switch (attr.type) {
            case INTEGER:
                if (!(value instanceof Integer)) {
                    throw new DBException("Type mismatch for '" + attr.name + "': expected INTEGER");
                }
                break;
            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw new DBException("Type mismatch for '" + attr.name + "': expected DOUBLE");
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw new DBException("Type mismatch for '" + attr.name + "': expected BOOLEAN");
                }
                break;
            case CHAR:
                if (!(value instanceof String)) {
                    throw new DBException("Type mismatch for '" + attr.name + "': expected CHAR");
                }
                // TODO: Check exact length when CHAR(N) length is stored in Attribute
                break;
            case VARCHAR:
                if (!(value instanceof String)) {
                    throw new DBException("Type mismatch for '" + attr.name + "': expected VARCHAR");
                }
                // TODO: Check max length when VARCHAR(N) length is stored in Attribute
                break;
        }
    }
}
