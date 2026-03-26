package parser;

import model.Record;
import model.Schema;
import model.Value;
import util.DBException;

@SuppressWarnings("all")
public class arithmeticNode implements IOperandNode{
    private IOperandNode leftNode;
    private IOperandNode rightNode;
    private String arithmeticOperation;

    public arithmeticNode(IOperandNode leftNode, IOperandNode rightNode, String arithmeticOperation){
        this.leftNode = leftNode;
        this.rightNode = rightNode;
        this.arithmeticOperation = arithmeticOperation;
    }

    @Override
    public Value getVal(Schema scheme, Record record) throws DBException {
//      TODO: Check if null
        Value leftVal = leftNode.getVal(scheme, record);
        Object rawLeft = leftVal.getRaw();
        Value rightVal = rightNode.getVal(scheme, record);
        Object rawRight = rightVal.getRaw();
        if(rawLeft == null || rawRight == null) {
            throw new DBException("One or more of the nodes is NULL. Unable to operate on NULL nodes");
        }
//      TODO: Check type
        if(!rawRight.getClass().equals(rawLeft.getClass())){
            throw new DBException("Operand types must match in order to do arithmetic operation");
        }
//      TODO: Have to handle ints and doubles
        if(rawLeft instanceof Integer){
            int leftInt= (Integer)rawLeft;
            int rightInt = (Integer)rawRight;
            if(arithmeticOperation.equals("+")){
                return new Value(leftInt + rightInt);
            }
            if(arithmeticOperation.equals("-")){
                return new Value(leftInt - rightInt);
            }
            if(arithmeticOperation.equals("*")){
                return new Value(leftInt * rightInt);
            }
            if(arithmeticOperation.equals("/")){
                if(rightInt == 0){
                    throw new DBException("Division by 0 is not allowed");
                }
                return new Value(leftInt / rightInt);
            }
            else{
                throw new DBException("Input operand not accepted");
            }
        }
        else if(rawLeft instanceof Double){
            double leftDoub = (Double)rawLeft;
            double rightDoub = (Double)rawRight;
            if(arithmeticOperation.equals("+")){
                return new Value(leftDoub + rightDoub);
            }
            if(arithmeticOperation.equals("-")){
                return new Value(leftDoub - rightDoub);
            }
            if(arithmeticOperation.equals("*")){
                return new Value(leftDoub * rightDoub);
            }
            if(arithmeticOperation.equals("/")){
                if(rightDoub == 0.0){
                    throw new DBException("Division by 0 is not allowed");
                }
                return new Value(leftDoub / rightDoub);
            }
        }
        throw new DBException("Only operations on types Integer and Double are permitted");
    }
}
