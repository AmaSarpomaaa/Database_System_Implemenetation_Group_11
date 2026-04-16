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

    public Attribute(String na, boolean nn, boolean primKey, Datatype typ){
        this(na, nn, primKey, primKey, typ, -1);
    }

    public Attribute(String na, boolean nn, boolean primKey, Datatype typ, int dataLen){
        this(na, nn, primKey, primKey, typ, dataLen);
    }

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
}
