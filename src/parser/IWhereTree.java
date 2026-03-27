package parser;
import model.Record;
import model.Schema;
import model.Value;
import util.DBException;
import util.ParseException;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface IWhereTree {

    static IWhereTree createWhereTree (String[] whereString) {

        Stack<String> operationStack = new Stack<>();
        Stack<Object> valueStack = new Stack<>();

        for (String expression : whereString) {

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
            switch (expression) {
                //relops
                case ">":
            }


        }

        throw new UnsupportedOperationException("Unimplemented method: createWhereTree");
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
