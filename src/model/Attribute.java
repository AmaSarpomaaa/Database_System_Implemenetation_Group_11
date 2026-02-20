package model;

public class Attribute {

    String name;
    boolean not_null;
    boolean unique;
    Datatype type;
    public Attribute(String na, boolean nn, boolean uniq, Datatype typ){
        name = na;
        not_null = nn;
        unique = uniq;
        type = typ;
    }

    public String getName() { return name; }
    public boolean isNotNull() { return not_null; }
    public boolean isPrimaryKey() { return unique; }
    public Datatype getType() { return type; }
}
