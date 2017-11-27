package adb_project;

import java.util.*;

public class Site {
    private int number;
    private ArrayList<Variable> variables;

    /*
      Java doesn't have tuple types so using a hashmap to emulate it:
      This is a hashmap with Variable as the key and it's value is
      a tuple-like record created using a single record hashmap.


      This looks like [Variable -> [TransactionID, WriteLock]]
    */
    private HashMap<Integer, HashMap<Transaction, String>> lockTable;
    private HashMap<Integer, Queue<Transaction>> lockQueue;
    private boolean isRunning;

    public Site(int num) {
        this.number = num;
        this.isRunning = true;
        this.variables = new ArrayList<>();
        this.lockTable = new HashMap<>();
        this.lockQueue = new HashMap<>();

        for (int i = 1; i <= 20; i++) {
            Variable v = new Variable(i);

            if (i % 2 == 0) {
                variables.add(v);
            } else if ((1 + i % 10) == num) {
                variables.add(v);
            }
            lockTable.put(i, new HashMap<>());
        }
    }

    public String toString() {
        String result = "";
        result += "Site: " + number;
        return result;
    }

    // check if site is up and running
    public Boolean isRunning() {
        return this.isRunning;
    }

    // Handles setting the failed / recover state of the site
    // Methods for recover / fail are at the DM now

    public void setSiteState(Boolean state) {
        this.isRunning = state;
    }

    public int getSiteNum() {
        return this.number;
    }

    // check if variable exists on this site by supplying variable object
    public Boolean hasVariable(Variable x) {
        for (Variable v: variables) {
            if (x.getId().equals(v.getId())) {
                return true;
            }
        }
        return false;
    }

    // check if variable exists on this site by supplying variable id
    public Boolean hasVariable(String id) {
        for (Variable v: variables) {
            if (id.equals(v.getId())) {
                return true;
            }
        }
        return false;
    }

    // get all variables on this site
    public ArrayList<Variable> getAllVariables() {
        return this.variables;
    }

    // get specific variable by supplying its id
    public Variable getVariable(String id) {
        return this.variables.get(Integer.parseInt(id));
    }

    // get variable's data by supplying its id
    public Integer getVariableData(String id) {
        return this.variables.get(Integer.parseInt(id)).getData();
    }

    public void updateVariable(String id, Integer value) {
        for (Variable v: variables) {
            if (id.equals(v.getId())) {
                v.updateData(value);
            }
        }
    }

    public void lockVariable(Transaction t, Integer varId, String lockType) {
            lockTable.get(varId).put(t, lockType);
    }

    public void unlockVariable(Transaction t, Integer varId) {
            lockTable.get(varId).remove(t);
    }

    public void handleLockQueue(Integer variable) {
        lockQueue.get(variable).remove();

    }


    public void addToLockQueue(Integer variable, Transaction transaction) {
        if(lockQueue.get(variable) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            lockQueue.put(variable, queue);
        } else {
            lockQueue.get(variable).add(transaction);
        }
    }

    public Transaction getFromLockQueue(Integer variable) {
        return lockQueue.get(variable).remove();
    }

    public boolean isVariableLocked(Integer varId) {
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
