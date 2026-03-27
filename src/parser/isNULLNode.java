package parser;

import model.Record;
import model.Schema;
import util.DBException;

public class isNULLNode implements IWhereTree{
    private IOperandNode node;

    public isNULLNode(IOperandNode node){
        this.node = node;
    }


    @Override
    public boolean evaluate(Schema scheme, Record record) throws DBException {
        return ((node.getVal(scheme, record)).getRaw() == null);
    }
}
