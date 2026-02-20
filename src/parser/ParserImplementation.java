package parser;

import model.*;
import util.ParseException;

import java.util.ArrayList;
import java.util.List;

public class ParserImplementation implements Parser {

    @Override
    public ParsedCommand parse(String input) throws ParseException {
        String s = input.trim();
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();

        if (s.startsWith("CREATE TABLE")) return parseCreateTable(s);
        if (s.startsWith("DROP TABLE")) return parseDropTable(s);
        if (s.startsWith("ALTER TABLE")) return parseAlterTable(s);
        if (s.startsWith("INSERT")) return parseInsert(s);
        if (s.startsWith("SELECT")) return parseSelect(s);

        throw new ParseException("Unrecognized command.");
    }

    // ---------------- CREATE TABLE ----------------
    private CreateTableCommand parseCreateTable(String s) throws ParseException {
        int open = s.indexOf('(');
        int close = s.lastIndexOf(')');
        if (open < 0 || close < 0 || close < open) throw new ParseException("Bad CREATE TABLE syntax.");

        String header = s.substring(0, open).trim();     // CREATE TABLE table1
        String inside = s.substring(open + 1, close).trim();

        String[] head = header.split("\\s+");
        if (head.length < 3) throw new ParseException("Bad CREATE TABLE syntax.");
        String tableName = head[2];

        String[] defs = inside.split(",");
        List<Attribute> attrs = new ArrayList<>();

        for (String def : defs) {
            String t = def.trim();
            if (t.isEmpty()) continue;

            String[] toks = t.split("\\s+");
            if (toks.length < 2) throw new ParseException("Bad attribute definition: " + t);

            String attrName = toks[0];
            Datatype type = parseDatatype(toks[1]);

            boolean primaryKey = false;
            boolean notNull = false;

            for (int i = 2; i < toks.length; i++) {
                if (toks[i].equals("PRIMARYKEY")) primaryKey = true;
                if (toks[i].equals("NOTNULL")) notNull = true;
            }

            // spec says PRIMARYKEY implies NOTNULL; your DDLParser doesn't auto-enforce it,
            // so we make it true here.
            if (primaryKey) notNull = true;

            // Attribute(name, notNull, unique/primaryKey, type)
            attrs.add(new Attribute(attrName, notNull, primaryKey, type));
        }

        return new CreateTableCommand(tableName, attrs.toArray(new Attribute[0]));
    }

    // ---------------- DROP TABLE ----------------
    private DropTableCommand parseDropTable(String s) throws ParseException {
        String[] toks = s.split("\\s+");
        if (toks.length != 3) throw new ParseException("Bad DROP TABLE syntax.");
        return new DropTableCommand(toks[2]);
    }

    // ---------------- ALTER TABLE ----------------
    private ParsedCommand parseAlterTable(String s) throws ParseException {
        String[] toks = s.split("\\s+");
        if (toks.length < 4) throw new ParseException("Bad ALTER TABLE syntax.");

        String tableName = toks[2];

        if (toks[3].equals("DROP")) {
            if (toks.length != 5) throw new ParseException("Bad ALTER TABLE DROP syntax.");
            return new AlterTableDropCommand(tableName, toks[4]);
        }

        if (toks[3].equals("ADD")) {
            if (toks.length < 6) throw new ParseException("Bad ALTER TABLE ADD syntax.");

            String attrName = toks[4];

            // allow: ADD abc DOUBLE;
            // also allow: ADD str CHAR(5) DEFAULT "hello";
            Datatype type = parseDatatype(toks[5]);

            boolean notNull = false;
            Object defaultValue = null;

            for (int i = 6; i < toks.length; i++) {
                if (toks[i].equals("NOTNULL")) notNull = true;
                if (toks[i].equals("DEFAULT")) {
                    if (i + 1 >= toks.length) throw new ParseException("DEFAULT missing value.");
                    defaultValue = parseLiteral(toks[i + 1], type);
                    break;
                }
            }

            Attribute attr = new Attribute(attrName, notNull, false, type);

            // AlterTableAddCommand(tableName, attribute, defaultValue) OR (tableName, attribute)
            if (defaultValue != null) return new AlterTableAddCommand(tableName, attr, defaultValue);
            return new AlterTableAddCommand(tableName, attr);
        }

        throw new ParseException("Bad ALTER TABLE syntax.");
    }

    // ---------------- INSERT ----------------
    private InsertCommand parseInsert(String s) throws ParseException {
        int valuesIdx = s.indexOf("VALUES");
        if (valuesIdx < 0) throw new ParseException("Bad INSERT syntax.");

        String before = s.substring(0, valuesIdx).trim(); // INSERT table1
        String after = s.substring(valuesIdx + "VALUES".length()).trim();

        String[] head = before.split("\\s+");
        if (head.length != 2) throw new ParseException("Bad INSERT syntax.");
        String tableName = head[1];

        int open = after.indexOf('(');
        int close = after.lastIndexOf(')');
        if (open < 0 || close < 0 || close < open) throw new ParseException("Bad INSERT VALUES syntax.");

        String inside = after.substring(open + 1, close).trim();

        // ✅ Each comma-separated item is ONE ROW (for Phase 1 / table1 scenario)
        List<String> items = splitTopLevelCommas(inside);

        InsertCommand cmd = new InsertCommand(tableName);

        for (int i = 0; i < items.size(); i++) {
            String tok = items.get(i).trim();
            if (tok.isEmpty()) continue;

            addInsertValue(cmd, tok);

            // ✅ Start a new row AFTER each value except the last
            if (i < items.size() - 1) {
                cmd.addRow();
            }
        }

        return cmd;
    }
    // ---------------- SELECT ----------------
    private ParsedCommand parseSelect(String s) throws ParseException {
        // Phase 1: SELECT * FROM tableName
        String[] toks = s.split("\\s+");
        if (toks.length != 4) throw new ParseException("Bad SELECT syntax.");
        if (!toks[1].equals("*")) throw new ParseException("Phase 1 only supports SELECT *.");
        if (!toks[2].equals("FROM")) throw new ParseException("Bad SELECT syntax.");
        return new SimpleSelectCommand(toks[3]);
    }

    // ---------------- helpers ----------------
    private Datatype parseDatatype(String token) throws ParseException {
        if (token.startsWith("CHAR")) return Datatype.CHAR;
        if (token.startsWith("VARCHAR")) return Datatype.VARCHAR;
        if (token.equals("INTEGER")) return Datatype.INTEGER;
        if (token.equals("DOUBLE")) return Datatype.DOUBLE;
        if (token.equals("BOOLEAN")) return Datatype.BOOLEAN;
        throw new ParseException("Unknown datatype: " + token);
    }

    private Object parseLiteral(String raw, Datatype type) {
        if (raw.equals("NULL")) return null;
        switch (type) {
            case INTEGER: return Integer.parseInt(raw);
            case DOUBLE: return Double.parseDouble(raw);
            case BOOLEAN: return raw.equals("True") ? Boolean.TRUE : Boolean.FALSE; // sample uses True/False
            case CHAR:
            case VARCHAR:
                return stripQuotes(raw);
            default:
                return raw;
        }
    }

    private void addInsertValue(InsertCommand cmd, String raw) throws ParseException {
        if (raw.equals("NULL")) { cmd.addNull(); return; }
        if (raw.equals("True")) { cmd.addBoolean(true); return; }
        if (raw.equals("False")) { cmd.addBoolean(false); return; }

        if (raw.startsWith("\"") && raw.endsWith("\"") && raw.length() >= 2) {
            cmd.addString(stripQuotes(raw));
            return;
        }

        // numeric: if it has a '.', treat as double
        if (raw.contains(".")) {
            try { cmd.addDouble(Double.parseDouble(raw)); return; }
            catch (NumberFormatException ignored) {}
        }

        try {
            cmd.addInteger(Integer.parseInt(raw));
            return;
        } catch (NumberFormatException e) {
            throw new ParseException("Bad value: " + raw);
        }
    }

    private String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) return s.substring(1, s.length() - 1);
        return s;
    }

    private List<String> splitTopLevelCommas(String s) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') inQuotes = !inQuotes;

            if (c == ',' && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) out.add(cur.toString().trim());
        return out;
    }

    private List<String> tokenizeRow(String row) throws ParseException {
        // split by whitespace but keep quoted strings together
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
                cur.append(c);
                continue;
            }

            if (Character.isWhitespace(c) && !inQuotes) {
                if (cur.length() > 0) {
                    out.add(cur.toString());
                    cur.setLength(0);
                }
            } else {
                cur.append(c);
            }
        }

        if (inQuotes) throw new ParseException("Unclosed string literal.");
        if (cur.length() > 0) out.add(cur.toString());

        return out;
    }
}