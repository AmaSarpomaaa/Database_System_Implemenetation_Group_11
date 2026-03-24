package parser;

import model.Record;
import model.Schema;
import util.DBException;

public class RelopNode implements IWhereTree{
    private IOperandNode left;
    private IOperandNode right;


    @Override
    public boolean evaluate(Schema scheme, Record record) throws DBException {
//        Check type if value or attribute node
//
        return false;
    }
}
