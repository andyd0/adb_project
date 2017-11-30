package adb_project;

import java.util.*;

public class Site {

    private int number;
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

    public Site(int num) {
        this.number = num;
        this.state = "running";
        this.variables = new HashMap<>();
        this.lockTable = new HashMap<>();

        for (int i = 1; i <= 20; i++) {
            Variable v = new Variable(i);

            if (i % 2 == 0) {
                variables.put("x" + i, v);
            } else if ((1 + i % 10) == num) {
                variables.put("x" + i, v);
            }
            lockTable.put("x" + i, new HashMap<>());
        }
    }

    public String toString() {
        String result = "";
        result += "Site: " + number;
        return result;
    }

    // check if site is up and running
    public Boolean isRunning() {
        return this.state.equals("running");
    }

    // Handles setting the failed / recover state of the site
    // Methods for recover / fail are at the DM now

    public void setSiteState(String state) {
        this.state = state;
    }

    public String getSiteState() {
        return state;
    }

    public int getSiteNum() {
        return this.number;
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
        return (this.variables.get(id) == null);
    }

    // get all variables on this site
    public HashMap<String, Variable> getAllVariables() {
        return this.variables;
    }

    // get specific variable by supplying its id
    public Variable getVariable(String id) {
        return this.variables.get(id);
    }

    // get variable's data by supplying its id
    public Integer getVariableData(String id) {
        return this.variables.get(id).getData();
    }

    public void updateVariable(String id, Integer value, Integer time) {
        this.variables.get(id).updateData(value, time);
    }

    public void lockVariable(Transaction t, String varId, Instruction instruction) {
        lockTable.get(varId).put(t, instruction);
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
    }

    public boolean isVariableLocked(String varId) {
        if(lockTable.get(varId).size() == 0) {
            return false;
        } else {
            return true;
        }
    }

    public void clearLocktable() {
        this.lockTable = new HashMap<>();
    }

//    public Transaction getLockingTransaction(String varId) {
//        for (Variable v: lockTable.keySet()) {
//            if (varId.equals(v.getId())) {
//                Iterator<Transaction> iterator =
//                  lockTable.get(v).keySet().iterator();
//                if (iterator.hasNext()) {
//                    return iterator.next();
//                }
//            }
//        }
//        return null;
//    }
}
