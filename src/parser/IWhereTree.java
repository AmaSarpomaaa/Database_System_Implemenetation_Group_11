package parser;
import model.*;
import model.Record;
import util.DBException;
import util.ParseException;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface IWhereTree {

    /**
     * Creates an IWhereTree based on a given array of expressions
     * @param expressions An array of expressions. Valid expressions include:
     *                    Values (INTEGER, DOUBLE, BOOLEAN, STRING)
     *                    AttributeNames
     *                    ArithmeticExpressions (+,-,*,/)
     *                    RelationalOperators (>, >=, <, <=, ==, <>)
     *                    AND, OR, IS, NULL
     * @return The IWhereTree that was created
     * @throws ParseException if the expressions represent an invalid WHERE clause
     * @throws DBException never
     */
    static IWhereTree createWhereTree (String[] expressions) throws ParseException, DBException {

        Stack<String> operationStack = new Stack<>();
        Stack<Object> valueStack = new Stack<>();

        for (String expression : expressions) {

            //check if expression is a value
            Value value;

            //check integer
            try {
                value = new Value(Integer.parseInt(expression));
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }
            catch (NumberFormatException ignored) {}

            //check double
            try {
                value = new Value(Double.parseDouble(expression));
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }
            catch (NumberFormatException ignored) {}

            //boolean
            if (expression.equals("True")) {
                value = new Value(true);
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }
            else if (expression.equals("False")) {
                value = new Value(false);
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }

            //null
            if (expression.equals("NULL")) {
                value = new Value(null);
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }

            //string
            Matcher charMatcher = Pattern.compile("\"([^\"]*)\"").matcher(expression);

            if (charMatcher.matches()) {
                value = new Value(charMatcher.group(1));
                ValueNode node = new ValueNode(value);
                valueStack.push(node);
                continue;
            }

            //if it gets here it's not a value node

            //check for operation node
            boolean opNode = false;

            switch (expression) {
                //arthmetic
                case "+": //all operations just fall through to make opNode true
                case "-":
                case "*":
                case "/":
                //relop
                case "==":
                case ">":
                case ">=":
                case "<":
                case "<=":
                case "<>":
                case "IS":
                //logical ops
                case "AND":
                case "OR": opNode = true; break;
                default: valueStack.push(new AttrNode(expression)); //is an attribute node
            }

            if (opNode) {
                //if it was an operation node, check if it can be pushed

                while (!(operationStack.isEmpty() || comparePriority(operationStack.peek(), expression) > 0)) {
                    String operation = operationStack.pop();
                    makeNode(operation, valueStack);
                }

                operationStack.push(expression);

            }

        }

        while (!operationStack.isEmpty()) {
            String operation = operationStack.pop();

            makeNode(operation, valueStack);
        }

        if (valueStack.size() != 1) {
            throw new ParseException("Invalid WHERE clause.");
        }

        return (IWhereTree) valueStack.pop();
    }

    /**
     * Helper function for createWhereTree. Creates a node, popping subnodes off of the value stack,
     * and puts the created node onto the value stack
     * @param operation The operation of the node that should be created
     * @param valueStack A stack containing IoperandNodes and IWhereTree nodes as used in the Shunting Yard Algorithm
     * @throws ParseException if the expressions represent an invalid WHERE clause
     * @throws DBException never
     */
    private static void makeNode(String operation, Stack<Object> valueStack) throws ParseException, DBException {
        String operationType = switch (operation) {
            //arithmetic operation
            case "+", "-", "*", "/" -> "arithOp";
            //relational operator
            case "==", ">", ">=", "<", "<=", "<>" -> "relOp";
            case "IS" -> "IS";
            //logical operators
            case "AND" -> "AND";
            case "OR" -> "OR";
            default -> throw new ParseException("Idk what went wrong here");
        };

        //create the appropriate node and push it onto the valueStack
        if (operationType.equals("arithOp")) {

            //get children off of valueStack
            if (valueStack.size() < 2) {
                throw new ParseException("Invalid WHERE clause.");
            }
            Object value1 = valueStack.pop();
            Object value2 = valueStack.pop();

            IOperandNode rightNode;
            IOperandNode leftNode;

            //convert objects to nodes
            if (value1 instanceof IOperandNode) {
                rightNode = (IOperandNode) value1;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }
            if (value2 instanceof IOperandNode) {
                leftNode = (IOperandNode) value2;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }

            //check for multiple operation attempts
            if (value1 instanceof arithmeticNode || value2 instanceof arithmeticNode) {
                throw new ParseException("JottQL only supports single-operation arithmetic");
            }

            valueStack.push(new arithmeticNode(leftNode, rightNode, operation));

        }
        else if (operationType.equals("relOp")) {

            //get children off of valueStack
            if (valueStack.size() < 2) {
                throw new ParseException("Invalid WHERE clause.");
            }
            Object value1 = valueStack.pop();
            Object value2 = valueStack.pop();

            IOperandNode rightNode;
            IOperandNode leftNode;

            //convert objects to nodes
            if (value1 instanceof IOperandNode) {
                rightNode = (IOperandNode) value1;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }
            if (value2 instanceof IOperandNode) {
                leftNode = (IOperandNode) value2;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }

            valueStack.push(new RelopNode(leftNode, rightNode, operation));

        }
        else if (operationType.equals("IS")) {

            //get child off of valueStack
            if (valueStack.size() < 2) {
                throw new ParseException("Invalid WHERE clause.");
            }
            Object value1 = valueStack.pop();
            Object value2 = valueStack.pop();

            IOperandNode rightNode;
            IOperandNode leftNode;

            //convert objects to nodes
            if (value1 instanceof ValueNode vn) {
                if (vn.value.getRaw() != null) {
                    throw new ParseException("IS must be followed by NULL");
                }
                rightNode = vn;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }
            if (value2 instanceof IOperandNode) {
                leftNode = (IOperandNode) value2;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }

            valueStack.push(new isNULLNode(leftNode));

        }
        else if (operationType.equals("AND")) {

            //get children off of valueStack
            if (valueStack.size() < 2) {
                throw new ParseException("Invalid WHERE clause.");
            }
            Object value1 = valueStack.pop();
            Object value2 = valueStack.pop();

            IWhereTree rightNode;
            IWhereTree leftNode;

            //convert objects to nodes
            if (value1 instanceof IWhereTree) {
                rightNode = (IWhereTree) value1;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }
            if (value2 instanceof IWhereTree) {
                leftNode = (IWhereTree) value2;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }

            valueStack.push(new ANDTree(leftNode, rightNode));

        }
        //OR
        else {
            //or
            //get children off of valueStack
            if (valueStack.size() < 2) {
                throw new ParseException("Invalid WHERE clause.");
            }
            Object value1 = valueStack.pop();
            Object value2 = valueStack.pop();

            IWhereTree rightNode;
            IWhereTree leftNode;

            //convert objects to nodes
            if (value1 instanceof IWhereTree) {
                rightNode = (IWhereTree) value1;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }
            if (value2 instanceof IWhereTree) {
                leftNode = (IWhereTree) value2;
            } else {
                throw new ParseException("Invalid WHERE clause.");
            }

            valueStack.push(new ORTree(leftNode, rightNode));

        }
    }

    /**
     * @return a positive number if op1 is higher priority than op2;
     * a negative number if op2 is higher priority than op2;
     * zero if they're equal
     */
    private static int comparePriority (String op1, String op2) throws ParseException {

        //split operator types into ranks that can be compared directly
        //lower ranks are lower priority

        int rank1 = switch (op1) {
            //arithmetic operators are all rank 1
            case "+", "-", "*", "/" -> 1;
            //relational operators are all rank 2
            case "==", ">", ">=", "<", "<=", "<>", "isNULL" -> 2;
            //logical ops
            case "AND" -> 3;
            case "OR" -> 4;
            default -> throw new ParseException("Idk what went wrong here");
        };

        int rank2 = switch (op2) {
            //arithmetic operators are all rank 1
            case "+", "-", "*", "/" -> 1;
            //relational operators are all rank 2
            case "==", ">", ">=", "<", "<=", "<>", "isNULL" -> 2;
            //logical ops
            case "AND" -> 3;
            case "OR" -> 4;
            default -> throw new ParseException("Idk what went wrong here");
        };

        return rank1 - rank2;
    }

    /**
     * Evaluates the node of the WHERE tree against a specific Record
     * @param scheme schema of the table used to find index of attr names
     * @param record row of the data being checked
     * @return true if record passes conditions | false if the record doesn't
     * @throws DBException I guess if there is an invalid column name or type mismatch
     */
    boolean evaluate(Schema scheme, Record record) throws DBException;

}
