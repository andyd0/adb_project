package adb_project;

import java.util.*;

public class Transaction {

    // TODO: Fileds will be updated once we write code for TM, DM
    private Integer id;
    private Boolean readOnly;
    private Integer startTime;
    private Integer lockAge;
    private Integer variable;
    private Boolean running;
    private Queue<Integer> variablesLocked;
    private HashMap<String, Instruction> variablesLockType;
    private HashMap<Integer, Integer> onSites;
    private Instruction currentInstruction;

    // variablesLocked is a queue keeping track of the variables that the transaction
    // has locked
    // currentInstructions is a queue so that when the transaction is unblocked the instructions
    // are executed

    public Transaction(Integer id, Boolean readOnly, Integer startTime, Integer variable, Instruction instruction) {

        this.id = id;
        this.readOnly = readOnly;
        this.startTime = startTime;
        this.lockAge = 0;
        this.variable = variable;
        this.running = true;
        this.variablesLocked = new LinkedList<>();
        this.variablesLockType = new HashMap<>();
        this.onSites = initializeOnSites();
        this.currentInstruction = instruction;
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

    public void addCurrentInstruction(Instruction instruction) {
        currentInstruction = instruction;
    }

    public Instruction getCurrentInstruction() {
        return currentInstruction;
    }

    //TODO: Should be updated to use string ids like other methods
    public void addLockedVariable(Integer variable) {
        variablesLocked.add(variable);
    }

    public Queue<Integer> getVariablesLocked() {
        return variablesLocked;
    }

    public void addLockedVariableType(String variable, Instruction instruction) {
        variablesLockType.put(variable, instruction);
    }

    public void removeLockedVariableType(String variable) {
        variablesLockType.remove(variable);
    }

    public Instruction getLockedVariableInfo(String variable) {
        return variablesLockType.get(variable);
    }

    public Boolean checkLockedVariableType(String variable) {
        return (variablesLockType.get(variable) != null && variablesLockType.get(variable).equals("W"));
    }

    public void stopTransaction() {
        running = false;
    }

    public Boolean isRunning() {
        return running;
    }

    public void setLockTime() {
        lockAge++;
    }

    public Integer getLockTime() {
        return lockAge;
    }

    public HashMap<Integer, Integer> getOnSites() {
        return onSites;
    }

    private HashMap<Integer, Integer> initializeOnSites() {

        HashMap<Integer, Integer> temp = new HashMap<>();

        for (int i = 1; i <= 10; i++) {
            temp.put(i, 0);
        }
        return temp;
    }

    public void plusOnSites(int siteId) {
        onSites.put(siteId, onSites.get(siteId) + 1);
    }

    public void decOnSites(int siteId) {
            onSites.put(siteId, onSites.get(siteId) - 1);
    }

    public String toString() {
        String result = "\nTransaction: " + this.id + "\n";
        result += "readOnly: " + this.readOnly + "\n";
        result += "startTime: " + this.startTime + "\n";
        result += "variable: " + this.variable + "\n";
        return result;
    }
}
