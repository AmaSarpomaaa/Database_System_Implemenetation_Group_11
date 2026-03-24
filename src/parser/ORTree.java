package parser;

import model.Record;
import model.Schema;
import util.DBException;

public class ORTree implements IWhereTree{
    private IWhereTree leftN;
    private IWhereTree rightN;

    public ORTree(IWhereTree leftN, IWhereTree rightN){
        this.leftN = leftN;
        this.rightN = rightN;
    }

    @Override
    public boolean evaluate(Schema scheme, Record record) throws DBException {
        return rightN.evaluate(scheme, record) || leftN.evaluate(scheme, record);
    }
}
