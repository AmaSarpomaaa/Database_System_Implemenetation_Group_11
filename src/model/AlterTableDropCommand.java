package model;

public class AlterTableDropCommand extends AlterTableCommand {

    public AlterTableDropCommand(String tableName, String attributeName) {
        super(tableName, attributeName);
    }

    @Override
    public boolean isAdd() {
        return false;
    }

    @Override
    public boolean isDrop() {
        return true;
    }

}