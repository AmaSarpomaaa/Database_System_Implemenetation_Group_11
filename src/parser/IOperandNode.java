package parser;

import model.Value;
import util.DBException;
import model.Record;
import model.Schema;

public interface IOperandNode {
    Value getVal(Schema scheme, Record record) throws DBException;

}
