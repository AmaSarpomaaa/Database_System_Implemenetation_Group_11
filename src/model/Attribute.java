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
}
