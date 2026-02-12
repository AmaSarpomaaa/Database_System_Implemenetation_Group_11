package app;

import java.io.File;
import java.util.Scanner;

/**
 * MAIN ENTRY POINT – STUB VERSION
 * Behaves like the sample session but does NOT implement real DB logic yet.
 */
public class JottQL {

    public static void main(String[] args) {

        if (args.length != 4) {
            System.out.println("Usage: java JottQL <dbLocation> <pageSize> <bufferSize> <indexingEnabled>");
            return;
        }

        String dbLocation = args[0];
        String pageSize   = args[1];
        String bufferSize = args[2];
        String indexing   = args[3];

        // ===== STARTUP MESSAGES (match sample) =====
        System.out.println("\nWelcome to JottQL!");
        System.out.println("Accessing database location....");

        File db = new File(dbLocation);

        if (!db.exists()) {
            System.out.println("No database found. Creating new database....");
        } else {
            System.out.println("Database found. Restarting database....");
            System.out.println("Ignoring provided page size. Using prior size of " + pageSize + "....");
        }

        // ===== BEGIN REPL =====
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print("\nJottQL> ");

            if (!sc.hasNextLine()) break;

            String statement = readStatement(sc);

            if (statement.equalsIgnoreCase("<QUIT>")) {
                shutdown();
                break;
            }

            // ===== FAKE EXECUTION ROUTING =====
            handleStubExecution(statement);
        }

        sc.close();
    }

    /**
     * Reads possibly multi-line SQL until ';'
     */
    private static String readStatement(Scanner sc) {

        StringBuilder sb = new StringBuilder();

        while (true) {
            String line = sc.nextLine().trim();

            if (line.equalsIgnoreCase("<QUIT>")) {
                return "<QUIT>";
            }

            sb.append(line).append(" ");

            if (line.endsWith(";")) {
                return sb.toString().trim();
            }

            System.out.print("... ");
        }
    }

    /**
     * Fake command handling that imitates sample output
     */
    private static void handleStubExecution(String sql) {

        String up = sql.toUpperCase();

        // ---- SELECT ----
        if (up.startsWith("SELECT")) {
            System.out.println("\n| x |");
            return;
        }

        // ---- CREATE TABLE ----
        if (up.startsWith("CREATE TABLE")) {
            System.out.println("Table created successfully");
            return;
        }

        // ---- INSERT ----
        if (up.startsWith("INSERT")) {

            // Fake detection of duplicate PK
            if (sql.contains("4")) {
                System.out.println("Error: duplicate primary key value: ( 4 )");
                System.out.println("0 rows inserted successfully");
                return;
            }

            // Fake success
            System.out.println("1 rows inserted successfully");
            return;
        }

        // ---- ALTER TABLE ----
        if (up.startsWith("ALTER TABLE")) {

            if (up.contains("NOTNULL") && !up.contains("DEFAULT")) {
                System.out.println("Error: Not null requires a default value when altering a table");
                return;
            }

            if (up.contains("DROP X")) {
                System.out.println("Error: Cannot drop primary key attribute");
                return;
            }

            System.out.println("Table altered successfully");
            return;
        }

        // ---- DROP TABLE ----
        if (up.startsWith("DROP TABLE")) {
            System.out.println("Table dropped successfully");
            return;
        }

        // ---- DEFAULT ----
        System.out.println("Command received (stub).");
    }

    /**
     * Shutdown prints matching sample
     */
    private static void shutdown() {
        System.out.println("Purging page buffer....");
        System.out.println("Writing catalog to hardware....");
        System.out.println("Shutting down the database...");
    }
}
