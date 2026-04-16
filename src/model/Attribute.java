package model;

public class Attribute {

    String name;
    boolean not_null;
    boolean unique;
    boolean primaryKey;
    Datatype type;
    /**
     * The length of the dataType if the data is of a type that has a length.
     * If the data is of a type that does not have a length, dataLength should
     * be -1.
     */
    private int dataLength;

    /**
     * Don't use this constructor. It's deprecated
     * Sets unique equal to the primKey value.
     * Automatically sets the Attribute to not have a dataLength regardless of the type
     * Equivalent to Attribute(na, nn, primKey, primKey, typ, -1)
     * @deprecated
     * @param na The name of the attribute.
     * @param nn true if the attribute has a not null constraint, false otherwise
     * @param primKey true if the attribute is a primaryKey, false otherwise
     *                The unique constraint is also set to the same value
     * @param typ The datatype of the attribute
     */
    @Deprecated
    public Attribute(String na, boolean nn, boolean primKey, Datatype typ){
        this(na, nn, primKey, primKey, typ, -1);
    }

    /**
     * Sets unique equal to the primKey value.
     * Equivalent to Attribute(na, nn, primKey, primKey, typ, dataLen)
     * @param na The name of the attribute.
     * @param nn true if the attribute has a not null constraint, false otherwise
     * @param primKey true if the attribute is a primaryKey, false otherwise
     *                The unique constraint is also set to the same value
     * @param typ The dataType of the attribute
     * @param dataLen The length of the dataType if the attribute is a CHAR or VARCHAR dataType.
     *                Ignored if typ is not CHAR or VARCHAR.
     */
    public Attribute(String na, boolean nn, boolean primKey, Datatype typ, int dataLen){
        this(na, nn, primKey, primKey, typ, dataLen);
    }

    /**
     * @param na The name of the attribute.
     * @param nn true if the attribute has a NOTNULL constraint, false otherwise
     * @param primKey true if the attribute is a PRIMARYKEY, false otherwise
     * @param uniq true if the attribute has a UNIQUE constraint, false otherwise
     * @param typ The dataType of the attribute
     * @param dataLen The length of the dataType if the attribute is a CHAR or VARCHAR dataType.
     *                Ignored if typ is not CHAR or VARCHAR.
     */
    public Attribute(String na, boolean nn, boolean primKey, boolean uniq, Datatype typ, int dataLen){
        name = na;
        not_null = nn;
        type = typ;

        if (primKey && !uniq) {
            //primary keys are always unique
            primaryKey = true;
            unique = true;
        }
        else {
            primaryKey = primKey;
            unique = uniq;
        }

        setDataLength(dataLen);
    }

    /**
     * Updates the dataLength if the data is of a type that has a length
     * (CHAR or VARCHAR). If the data is a type that does not have a length,
     * sets the dataLength to -1.
     * Either way, the dataLength that was set is returned.
     * @param dataLen The value to set the dataLength to.
     * @return the value that the dataLength was set to.
     */
    public int setDataLength(int dataLen) {

        if (type == Datatype.CHAR || type == Datatype.VARCHAR) {
            dataLength = dataLen;
        }
        else {
            dataLength = -1;
        }

        return dataLength;
    }

    public int getDataLength() {
        return dataLength;
    }

    public String getName() { return name; }
    public boolean isNotNull() { return not_null; }

    public boolean isUnique() {
        return unique;
    }

    public boolean isPrimaryKey() { return primaryKey; }
    public Datatype getType() { return type; }

    public void setUnique(boolean u){
        unique = u;
    }

    public void setPrimaryKey(boolean primaryKey){
        this.primaryKey = primaryKey;
        if (primaryKey) {
            this.unique = true;
        }
    }
}
