package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Result {
    private final boolean success;
    private final String message;
    private final List<Record> records;
    private final int rowsAffected;

    private Result(boolean success, String message, List<Record> records, int rowsAffected) {
        this.success = success;
        this.message = message;
        this.records = (records == null) ? new ArrayList<>() : new ArrayList<>(records);
        this.rowsAffected = rowsAffected;
    }

    public static Result ok(String message) {
        return new Result(true, message, null, 0);
    }

    public static Result ok(String message, int rowsAffected) {
        return new Result(true, message, null, rowsAffected);
    }

    public static Result okWithRecords(String message, List<Record> records) {
        return new Result(true, message, records, (records == null ? 0 : records.size()));
    }

    public static Result error(String message) {
        return new Result(false, message, null, 0);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public List<Record> getRecords() {
        return Collections.unmodifiableList(records);
    }

    public int getRowsAffected() {
        return rowsAffected;
    }
}
