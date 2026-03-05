package app;

import engine.DBEngine;
import engine.SimpleDBEngine;
import util.DBException;

import java.io.File;
import java.util.Scanner;
import model.Result;

public class JottQL {

    public static void main(String[] args) {

        if (args.length != 4) {
            System.out.println("Usage: java JottQL <dbLocation> <pageSize> <bufferSize> <indexing>");
            return;
        }

        String dbLocation = args[0];

        int pageSize;
        int bufferSize;
        boolean indexingEnabled;

        try {
            pageSize = Integer.parseInt(args[1]);
            bufferSize = Integer.parseInt(args[2]);
            indexingEnabled = Boolean.parseBoolean(args[3]);
        } catch (Exception e) {
            System.out.println("Error: invalid arguments.");
            System.out.println("Usage: java JottQL <dbLocation> <pageSize:int> <bufferSize:int> <indexing:true|false>");
            return;
        }

        // ===== STARTUP MESSAGES =====
        System.out.println("\nWelcome to JottQL!");
        System.out.println("Accessing database location....");

        File db = new File(dbLocation);

        if (!db.exists()) {
            System.out.println("No database found. Creating new database....");
        } else {
            System.out.println("Database found. Restarting database....");
            System.out.println("Ignoring provided page size. Using prior size of " + pageSize + "....");
        }

        // ===== REAL STARTUP =====
        DBEngine engine = new SimpleDBEngine();
        try {
            engine.startup(dbLocation, pageSize, bufferSize, indexingEnabled);
        } catch (DBException e) {
            System.out.println("Fatal startup error: " + e.getMessage());
            return;
        }

        // ===== REPL =====
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.print("\nJottQL> ");
            if (!sc.hasNextLine()) break;

            String statement = readStatement(sc);

            if (statement.equalsIgnoreCase("<QUIT>")) {
                // Spec-required shutdown prints
                System.out.println("Purging page buffer....");
                System.out.println("Writing catalog to hardware....");
                System.out.println("Shutting down the database...");

                try {
                    engine.shutdown();
                } catch (DBException e) {
                    System.out.println("Shutdown error: " + e.getMessage());
                }
                break;
            }

            try {
                SimpleDBEngine simpleEngine = (SimpleDBEngine) engine;
                Result result = simpleEngine.execute(statement);

                if (result != null && result.getMessage() != null) {
                    System.out.println(result.getMessage());
                }

            } catch (DBException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        sc.close();
    }

    /**
     * Reads possibly multi-line SQL until ';'
     */
    private static String readStatement(Scanner sc) {
        StringBuilder sb = new StringBuilder();

        while (true) {
            String line = sc.nextLine();

            if (line == null) return "";
            line = line.trim();

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
}