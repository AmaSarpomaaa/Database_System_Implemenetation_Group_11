package model;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private final List<Value> attributes;

    public Record(){
        this.attributes = new ArrayList<>();
    }

    public Record(List<Value> attributes){
        this.attributes = new ArrayList<>();
    }

    public List<Value> getAttributes() {
        return attributes;
    }

    public void addAttribute(Value o){
        attributes.add(o);
    }

    public Value getValue(int index){
        return attributes.get(index);
    }

}
