package parser;

import model.Record;
import model.Schema;
import util.DBException;

public class ANDTree implements IWhereTree{
    private IWhereTree leftN;
    private IWhereTree rightN;

    public ANDTree(IWhereTree leftN, IWhereTree rightN){
        this.leftN = leftN;
        this.rightN = rightN;
    }

    @Override
    public boolean evaluate(Schema scheme, Record record) throws DBException {
//        The implementation would go here
        return false;
    }
}
