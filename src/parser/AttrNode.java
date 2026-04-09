package parser;

import model.Attribute;
import model.Record;
import model.Schema;
import model.Value;
import util.DBException;

import java.util.List;

public class AttrNode implements IOperandNode{
    public final String attrName;

    public AttrNode(String attrName){
        this.attrName = attrName;
    }


    @Override
    public Value getVal(Schema scheme, Record record) throws DBException {
        // Try exact match first
        int attrIndex = scheme.getAttributeIndex(attrName);
        if (attrIndex != -1) {
            return record.getValue(attrIndex);
        }

        // Try suffix match for qualified names (e.g. "i1" matches "t2.i1")
        List<Attribute> attrs = scheme.getAttributes();
        for (int i = 0; i < attrs.size(); i++) {
            if (attrs.get(i).getName().endsWith("." + attrName)) {
                return record.getValue(i);
            }
        }

        throw new DBException("Attribute {" + attrName + "} not found. Return value of -1.");
    }
}
