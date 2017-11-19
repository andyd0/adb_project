package adb_project;

public class Transaction {

    private Integer id;
    private Boolean readOnly;
    private Integer startTime;
    private Integer variable;

    public void transaction(Integer id, Boolean readOnly, Integer startTime, Integer variable) {
        this.id = id;
        this.readOnly = readOnly;
        this.startTime = startTime;
        this.variable = variable;
    }

    public Integer getID () {
        return id;
    }
    public Integer getStartTime () {
        return startTime;
    }

    public Integer getVariable () {
        return variable;
    }

    public Boolean isReadOnly () {
        return readOnly;
    }
}
