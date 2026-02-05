package model;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Object> attributes;

    public Record(){
        attributes = new ArrayList<Object>();
    }

    public List<Object> getAttributes() {
        return attributes;
    }

    public void addAttribute(Object o){
        attributes.add(o);
    }

}
