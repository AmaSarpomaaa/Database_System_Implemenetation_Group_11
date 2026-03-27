package model;

public class Condition {

    private final String attribute;
    private final Object value;

    public Condition(String attribute, Object value) {
        this.attribute = attribute;
        this.value = value;
    }

    public String getAttribute() {
        return attribute;
    }

    public Object getValue() {
        return value;
    }
}