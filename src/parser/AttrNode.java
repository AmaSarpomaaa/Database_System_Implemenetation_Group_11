package parser;

import model.Record;
import model.Schema;
import model.Value;
import util.DBException;

public class AttrNode implements IOperandNode{
    public final String attrName;

    public AttrNode(String attrName){
        this.attrName = attrName;
    }


    @Override
    public Value getVal(Schema scheme, Record record) throws DBException {
        int attrIndex = scheme.getAttributeIndex(attrName);
        if(attrIndex != -1){
            return record.getValue(attrIndex);
        }
        throw new DBException("Attribute {" + attrName + "} not found. Return value of -1.");
    }
}
