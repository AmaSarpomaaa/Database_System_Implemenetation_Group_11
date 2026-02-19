package model;

public class Attribute {

    String name;
    boolean not_null;
    boolean unique;
    Datatype type;
    private final int length;

    public Attribute(String na, boolean nn, boolean uniq, Datatype typ){
        name = na;
        not_null = nn;
        unique = uniq;
        type = typ;
        this.length = -1;
    }

    public Attribute(String na, boolean nn, boolean uniq, Datatype typ, int len){
        name = na;
        not_null = nn;
        unique = uniq;
        type = typ;
        this.length = len;
    }


    public String getName() { return name; }
    public boolean isNotNull() { return not_null; }
    public boolean isUnique() { return unique; }
    public Datatype getType() { return type; }
    public int getLength() { return length; }
}
