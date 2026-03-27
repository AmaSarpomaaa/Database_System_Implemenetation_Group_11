package parser;
import model.Record;
import model.Schema;
import util.DBException;

public interface IWhereTree {
    /**
     * Evaluates the node of the WHERE tree against a specific Record
     * @param scheme schema of the table used to find index of attr names
     * @param record row of the data being checked
     * @return true if record passes conditions | false if the record doesn't
     * @throws DBException I guess if there is an invalid column name or type mismatch
     */
    boolean evaluate(Schema scheme, Record record) throws DBException;
}
