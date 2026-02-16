package model;

import java.util.List;

public class Schema {
    List<Attribute> attributeList;

    public Schema(List<Attribute> attr){
        attributeList = attr;
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }
}
