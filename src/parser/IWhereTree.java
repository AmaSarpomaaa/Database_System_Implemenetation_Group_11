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

    public static IWhereTree createWhereTree (String[] whereString) {

        Stack<String> operationStack = new Stack<>();
        Stack<IWhereTree> valueStack = new Stack<>();

        for (String expression : whereString) {

            //check if expression is a value
            Value value;

            //check integer
            try {
                value = new Value(Integer.parseInt(expression));
                ValueNode node = new ValueNode(value);
                valueStack.push();
                continue;
            }
            catch (NumberFormatException ignored) {}

            //check double
            try {
                value = new Value(Double.parseDouble(expression));
                continue;
            }
            catch (NumberFormatException ignored) {}

            //boolean
            if (expression.equals("True")) {
                value = new Value(true);
                continue;
            }
            else if (expression.equals("False")) {
                value = new Value(false);
                continue;
            }

            //null
            if (expression.equals("NULL")) {
                value = new Value(null);
                continue;
            }

            //string
            Matcher charMatcher = Pattern.compile("\"([^\"]*)\"").matcher(expression);

            if (charMatcher.matches()) {
                value = new Value(charMatcher.group(1));
                continue;
            }

            //if it gets here its not a value node


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
