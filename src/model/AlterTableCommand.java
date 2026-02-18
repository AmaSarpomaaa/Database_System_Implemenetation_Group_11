package model;

import parser.CommandType;

/**
 * An abstract class to represent a ParsedCommand for an Alter statement.<br>
 * ParsedCommands that are found to be AlterTableCommands should use isAdd() or
 * isDrop() to determine whether to use the AlterTableAddCommand or
 * AlterTableDropCommand subclass to process the command.<br>
 * Subclasses that extend this class should return true in all cases for
 * exactly one of isAdd() and isDrop().
 */
public abstract class AlterTableCommand extends ParsedCommand {

    private final String tableName;
    private final String attributeName;

    public AlterTableCommand(String tableName, String attributeName) {
        this.tableName = tableName;
        this.attributeName = attributeName;
    }

    @Override
    public CommandType getType() {
        return CommandType.ALTER;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Exactly one of isAdd and isDrop should return true for any subclass of
     * AlterTableCommand.
     * @return true if the command is an ADD command, false otherwise
     */
    public abstract boolean isAdd();

    /**
     * Exactly one of isAdd and isDrop should return true for any subclass of
     * AlterTableCommand.
     * @return true if the command is a DROP command, false otherwise
     */
    public abstract boolean isDrop();
}