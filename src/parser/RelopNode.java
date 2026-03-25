package parser;

import model.Record;
import model.Schema;
import model.Value;
import util.DBException;

public class RelopNode implements IWhereTree{
    private IOperandNode left;
    private IOperandNode right;
    private String operator;


    public RelopNode(IOperandNode left, IOperandNode right, String operator){
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    @SuppressWarnings("all")
    @Override
    public boolean evaluate(Schema scheme, Record record) throws DBException {
        //TODO: check if this line works or if I have to separate it into two variables
        Object leftValRaw= (left.getVal(scheme, record)).getRaw();
        Object rightValRaw= (right.getVal(scheme, record)).getRaw();

        //TODO: check for null values
        if(leftValRaw == null || rightValRaw == null){
            throw new DBException("NULL values can not be evaluated");
        }
        if(leftValRaw.getClass() != rightValRaw.getClass()){
            throw new DBException("Left node and right node are not of the same type");
        }

        Comparable<Object> compRight = (Comparable<Object>) rightValRaw;
        int comparable = compRight.compareTo(leftValRaw);
        if(operator.equals("==")){
            return comparable == 0;
        }
        if(operator.equals("<>")){
            return comparable != 0;
        }
        if(operator.equals("<")){
            return comparable < 0;
        }
        if(operator.equals(">")){
            return comparable > 0;
        }
        if(operator.equals("<=")){
            return comparable <= 0;
        }
        if(operator.equals(">=")){
            return comparable >= 0;
        }
        else{
            throw new DBException("Operator provided is invalid:" + operator);
        }
    }
}
