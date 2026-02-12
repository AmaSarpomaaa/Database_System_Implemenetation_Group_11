package model;

public class Value {
    private final Object raw;

    public Value(Object raw) {
        this.raw = raw;
    }

    public Object getRaw() {
        return raw;
    }

    @Override
    public String toString() {
        return (raw == null) ? "NULL" : raw.toString();
    }
}
