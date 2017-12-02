package adb_project;

import java.util.*;

public class Site {

    private Integer id;
    private HashMap<String, Variable> variables;

    /*
      Java doesn't have tuple types so using a hashmap to emulate it:
      This is a hashmap with Variable as the key and it's value is
      a tuple-like record created using a single record hashmap.


      This looks like [Variable -> [TransactionID, Instruction]]

      TODO: State for a site is 4 - failed, recovered, recovered and written to and running.  Handle with string?

      States: "running", "failed", "recovered", "recovered_written"
    */
    private HashMap<String, HashMap<Transaction, Instruction>> lockTable;
    private String state;

    public Site(Integer id) {
        this.id = id;
        this.state = "running";
        this.variables = new HashMap<>();
        this.lockTable = initializeLockTable();

        for (int i = 1; i <= 20; i++) {
            Variable v = new Variable(i);

            if (i % 2 == 0) {
                variables.put("x" + i, v);
            } else if ((1 + i % 10) == id) {
                variables.put("x" + i, v);
            }
        }
    }

    public String toString() {
        String result = "";
        result += "Site: " + id;
        return result;
    }

    // Handles setting the failed / recover state of the site
    // Methods for recover / fail are at the DM now

    public void setSiteState(String state) {
        this.state = state;
    }

    public String getSiteState() {
        return state;
    }

    public Integer getId() {
        return id;
    }

    // check if variable exists on this site by supplying variable object
    public Boolean hasVariable(Variable x) {

        for (HashMap.Entry<String, Variable> entry : this.variables.entrySet())
        {
            if(x.getId().equals(entry.getValue().getId())) {
                return true;
            }
        }
        return false;
    }

    // check if variable exists on this site by supplying variable id
    public Boolean hasVariable(String id) {
        return (variables.get(id) == null);
    }

    // get all variables on this site
    public HashMap<String, Variable> getAllVariables() {
        return variables;
    }

    // get specific variable by supplying its id
    public Variable getVariable(String id) {
        return variables.get(id);
    }

    // get variable's data by supplying its id
    public Integer getVariableValue(String id) {
        return variables.get(id).getValue();
    }

    public void updateVariable(String id, Integer value, Integer time) {
        variables.get(id).updateValue(value, time);
        variables.get(id).valueCommitted();
        if(!variables.get(id).getOkToRead()) {
            variables.get(id).setOkToRead(true);
        }
    }

    public void lockVariable(Transaction t, String varId, Instruction instruction) {
        lockTable.get(varId).put(t, instruction);
        t.plusOnSites(this.id);
    }

    // When a transaction commits, checks to see if it was a write
    // instruction and updates value.  For both R/W it will remove
    // from lock queue
    public void handleLockTable(Transaction t, String varId, Integer time) {
        Instruction instruction = lockTable.get(varId).get(t);
         if(instruction.getInstruction().equals("W")) {
            updateVariable(varId, instruction.getValue(), time);
        }
        lockTable.get(varId).remove(t);
        t.decOnSites(this.id);
    }

    public boolean isVariableWriteLocked(String varId) {

        if(lockTable.size() != 0 && lockTable.get(varId).size() != 0) {
            if (lockTable.get(varId).entrySet().iterator().next().getValue().getInstruction().equals("R")) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    public boolean isVariableLocked(String varId) {
        return (lockTable.get(varId).size() != 0);
    }

    public void clearLocktable() {
        this.lockTable = new HashMap<>();
    }

    private HashMap<String, HashMap<Transaction, Instruction>> initializeLockTable() {

        HashMap<String, HashMap<Transaction, Instruction>> temp = new HashMap<>();

        for (int i = 1; i <= 20; i++) {
            temp.put("x" + i, new HashMap<>());
        }
        return temp;
    }

    public Set<Transaction> getLockedTransactions(){
        Set<Transaction> lockedTransactions = new HashSet<>();
        for (HashMap.Entry<String, HashMap<Transaction, Instruction>> entry : this.lockTable.entrySet()) {
            for (HashMap.Entry<Transaction, Instruction> tEntry : entry.getValue().entrySet()) {
                lockedTransactions.add(tEntry.getKey());
            }
        }
        return lockedTransactions;
    }

    public void removeFromLockTable(Transaction T) {
        for (HashMap.Entry<String, HashMap<Transaction, Instruction>> entry : this.lockTable.entrySet()) {
            for (HashMap.Entry<Transaction, Instruction> tEntry : entry.getValue().entrySet()) {
                if(T.getID().equals(tEntry.getKey().getID())) {
                    entry.getValue().remove(T);
                }
            }
        }
    }

    public Integer getVariableCount() {
        return variables.size();
    }

    public void recover() {
        for (HashMap.Entry<String, Variable> entry : this.variables.entrySet()) {
            Integer id = Integer.parseInt(entry.getKey().replaceAll("\\D+",""));
            if(id % 2 == 0) {
                entry.getValue().setOkToRead(false);
            } else {
                entry.getValue().setOkToRead(true);
            }
        }
        lockTable = initializeLockTable();
    }
}
