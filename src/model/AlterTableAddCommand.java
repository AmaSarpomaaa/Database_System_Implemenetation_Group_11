package model;

public class AlterTableAddCommand extends AlterTableCommand {

    private final Attribute attribute;
    /**
     * defaultValue should always be of the type defined in dataType or null.
     * If there is no defaultValue, defaultValue is null.
     */
    private final Object defaultValue;

    public AlterTableAddCommand(String tableName, Attribute attribute, Object defaultValue) {
        super(tableName, attribute.name);
        this.attribute = attribute;
        this.defaultValue = defaultValue;
    }

    public AlterTableAddCommand(String tableName, Attribute attribute) {
        this(tableName, attribute, null);
    }

    @Override
    public boolean isAdd() {
        return true;
    }

    @Override
    public boolean isDrop() {
        return false;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public Datatype getDataType() {
        return attribute.type;
    }

    public boolean isNotNull() {
        return attribute.not_null;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

}