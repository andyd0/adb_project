package adb_project;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Transaction {

    // TODO: Fileds will be updated once we write code for TM, DM
    private Integer id;
    private Boolean readOnly;
    private Integer startTime;
    private Integer variable;
    private Boolean running;
    private Queue<Integer> variablesLocked;
    private Instruction currentInstruction;

    // variablesLocked is a queue keeping track of the variables that the transaction
    // has locked
    // currentInstructions is a queue so that when the transaction is unblocked the instructions
    // are executed

    public Transaction(Integer id, Boolean readOnly, Integer startTime, Integer variable, Instruction instruction) {

        this.id = id;
        this.readOnly = readOnly;
        this.startTime = startTime;
        this.variable = variable;
        this.running = true;
        this.variablesLocked = new LinkedList<>();
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

    public void addLockedVariable(Integer variable) {
        variablesLocked.add(variable);
    }
    public Queue<Integer> getVariablesLocked() {
        return variablesLocked;
    }

    public void stopTransaction() {
        this.running = false;
    }

    public String toString() {
        String result = "\nTransaction: " + this.id + "\n";
        result += "readOnly: " + this.readOnly + "\n";
        result += "startTime: " + this.startTime + "\n";
        result += "variable: " + this.variable + "\n";
        return result;
    }
}
