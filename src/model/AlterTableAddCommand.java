package model;

public class AlterTableAddCommand extends AlterTableCommand {

    private final Datatype dataType;
    private final boolean notNull;
    /**
     * defaultValue should always be of the type defined in dataType or null.
     * If there is no defaultValue, defaultValue is null.
     */
    private final Object defaultValue;

    public AlterTableAddCommand(String tableName, String attributeName,
                                Datatype dataType, boolean notNull, Object defaultValue) {
        super(tableName, attributeName);
        this.dataType = dataType;
        this.notNull = notNull;
        this.defaultValue = defaultValue;
    }

    public AlterTableAddCommand(String tableName, String attributeName,
                                Datatype dataType, boolean notNull) {
        this(tableName, attributeName, dataType, notNull, null);
    }

    @Override
    public boolean isAdd() {
        return false;
    }

    @Override
    public boolean isDrop() {
        return true;
    }

    public Datatype getDataType() {
        return dataType;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

}