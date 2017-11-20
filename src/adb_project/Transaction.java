package adb_project;

public class Transaction {

    private Integer id;
    private Boolean readOnly;
    private Integer startTime;
    private Integer variable;
    private Integer index;
    private Boolean running;

    public Transaction(Integer id, Boolean readOnly, Integer startTime, Integer variable, Integer index) {
        this.id = id;
        this.readOnly = readOnly;
        this.startTime = startTime;
        this.variable = variable;
        this.index = index;
        this.running = true;
    }

    public Integer getID() {
        return id;
    }

    public Boolean isReadOnly() {
        return readOnly;
    }

    public Integer getStartTime() {
        return startTime;
    }

    public Integer getVariable() {
        return variable;
    }

    public Integer getIndex() {
        return index;
    }

    public void stopTransaction() {
        this.running = false;
    }

}