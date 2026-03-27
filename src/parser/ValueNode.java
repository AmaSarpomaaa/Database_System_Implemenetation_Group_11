package parser;

import model.Record;
import model.Schema;
import model.Value;
import util.DBException;

public class ValueNode implements IOperandNode{

    public final Value value;

    public ValueNode(Value value){
        this.value = value;
    }

    @Override
    public Value getVal(Schema scheme, Record record) throws DBException {
        return value;
    }
}