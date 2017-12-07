/**
 * <h1>Site</h1>
 * Methods and Construction for a site
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

import java.util.*;

public class Site {

    private Integer id;
    private HashMap<String, Variable> variables;
    private HashMap<String, HashMap<Transaction, Instruction>> lockTable;
    private String state;

    /**
     * Creates a Site object
     * @param id - site ID
     *
     * <ul>
     *      <li>variables - variables on the site</li>
     *      <li>lockTable - Java doesn't have tuple types so using a hashmap to emulate it:
     *          This is a hashmap with Variable as the key and it's value is
     *          a tuple-like record created using a single record hashmap.
     *          This looks like [Variable -> [TransactionID, Instruction]]</li>
     *      <li>Possible states for a Site - running, failed, recovered</li>
     * </ul>
     */
    public Site(Integer id) {
        this.id = id;
        this.state = "running";
        this.variables = new HashMap<>();
        this.lockTable = initializeLockTable();

        // Only adds applicable variables to the site
        for (int i = 1; i <= 20; i++) {
            Variable v = new Variable(i);

            if (i % 2 == 0) {
                variables.put("x" + i, v);
            } else if ((1 + i % 10) == id) {
                variables.put("x" + i, v);
            }
        }
    }

    /**
     * Sets the current state of a transaction
     * @param state - string of site's current state
     */
    public void setSiteState(String state) {
        this.state = state;
    }

    /**
     * Gets the current state of a site
     * @return String - gets the current state of a site
     */
    public String getSiteState() {
        return state;
    }

    /**
     * Gets the site ID
     * @return Integer - gets the site ID
     */
    public Integer getId() {
        return id;
    }

    /**
     * HashMap of all variables on this site
     * @return HashMap
     */
    public HashMap<String, Variable> getAllVariables() {
        return variables;
    }

    /**
     * Gets specific variable by supplying its id
     * @param id - variable ID string
     * @return Variable - variable object
     */
    public Variable getVariable(String id) {
        return variables.get(id);
    }

    /**
     * Updates a variable's value at transaction commit
     * @param id - variable ID string
     * @param value - value of variable
     * @param time - time when variable was updated
     */
    public void updateVariable(String id, Integer value, Integer time) {
        variables.get(id).updateValue(value, time);
        variables.get(id).valueCommitted();
        if(!variables.get(id).getOkToRead()) {
            variables.get(id).setOkToRead(true);
        }
    }

    /**
     * Adds a transaction to a variable's lock table HashMap
     * @param t - transaction object
     * @param varId - variable ID
     * @param instruction - instruction object
     */
    public void lockVariable(Transaction t, String varId, Instruction instruction) {
        lockTable.get(varId).put(t, instruction);
        t.plusOnSites(this.id);
    }

    /**
     * When a transaction commits, checks to see if it was a write instruction and updates value.
     * For both R/W it will remove from lock queue
     * @param t - transaction object
     * @param varId - variable ID
     * @param time - time when variable is updated
     */
    public void handleLockTable(Transaction t, String varId, Integer time) {
        Instruction instruction = lockTable.get(varId).get(t);
        if(instruction.getInstruction().equals("W")) {
            updateVariable(varId, instruction.getValue(), time);
        }
        lockTable.get(varId).remove(t);
        t.removeLockedVariableType(varId);
        t.decOnSites(this.id);
    }

    /**
     * Checks to see whether a variable is write locked.  Really just to handle
     * the times where a transaction is trying to read a variable it has a write
     * lock on
     * @param varId - variable ID
     * @return Boolean - true/false if variable is write locked
     */
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

    /**
     * Checks to see whether a variable is locked.
     * @param varId - variable ID
     * @return Boolean - true/false if variable is locked
     */
    public boolean isVariableLocked(String varId) {
        return (lockTable.get(varId).size() != 0);
    }

    /**
     * Clears the lock table when a site fails
     */
    public void clearLocktable() {
        this.lockTable = new HashMap<>();
    }

    // TODO: This should not create a table for all 20 variables.  Just relevant ones
    /**
     * Initializes lock table
     * @return HashMap - Nested HashMap - variable and it's transaction / instruction
     */
    private HashMap<String, HashMap<Transaction, Instruction>> initializeLockTable() {

        HashMap<String, HashMap<Transaction, Instruction>> temp = new HashMap<>();

        for (int i = 1; i <= 20; i++) {
            temp.put("x" + i, new HashMap<>());
        }
//        // Only adds applicable variables to the site
//        for (int i = 1; i <= 20; i++) {
//            if (i % 2 == 0) {
//                temp.put("x" + i, new HashMap<>());
//            } else if ((1 + i % 10) == id) {
//                temp.put("x" + i, new HashMap<>());
//            }
//        }
        return temp;
    }

    /**
     * Returns a set of transactions that have locks on a site
     * @return Set - locked transaction objects
     */
    public Set<Transaction> getLockedTransactions(){
        Set<Transaction> lockedTransactions = new HashSet<>();
        for (HashMap.Entry<String, HashMap<Transaction, Instruction>> entry : this.lockTable.entrySet()) {
            for (HashMap.Entry<Transaction, Instruction> tEntry : entry.getValue().entrySet()) {
                lockedTransactions.add(tEntry.getKey());
            }
        }
        return lockedTransactions;
    }

    /**
     * Removes a transaction from the site's lock table
     * @param T - transaction object
     */
    public void removeFromLockTable(Transaction T) {
        for (HashMap.Entry<String, HashMap<Transaction, Instruction>> entry : this.lockTable.entrySet()) {
            for (Iterator<HashMap.Entry<Transaction, Instruction>> it =
                 entry.getValue().entrySet().iterator(); it.hasNext();) {
                Transaction t = it.next().getKey();
                if (T.getID().equals(t.getID())) {
                    it.remove();
                }
            }
        }
    }

    /**
     * Count of variables on the site
     * @return Integer - count of variables on the site
     */
    public Integer getVariableCount() {
        return variables.size();
    }

    /**
     * When a site is recoved, variables are set to whether they can be
     * read from (odd) or not (even).  Also initializes the lock table
     * since it was wiped out when the site failed
     */
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

    public Transaction getTransactionThatLockedVariable(String varId) {
        HashMap < Transaction, Instruction > map = this.lockTable.get(varId);
        Transaction t = null;
        if (map == null) {
            return null;
        }
        for (HashMap.Entry < Transaction, Instruction > entry: map.entrySet()) {
            t = entry.getKey();
        }
        return t;
    }

    /**
     * toString method for a site object
     * @return String - details of a Site
     */
    public String toString() {
        String result = "";
        result += "Site: " + id;
        return result;
    }
}
