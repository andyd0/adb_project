/**
 * <h1>Transaction</h1>
 * Methods and Construction for a transaction
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

import java.util.*;

public class Transaction {

    private Integer id;
    private Boolean readOnly;
    private Integer startTime;
    private Boolean running;
    private HashMap<String, LinkedList<Transaction>> dependsOnIt;
    private HashMap<String, LinkedList<Transaction>> dependsOn;
    private Queue<String> variablesLocked;
    private HashMap<String, Instruction> variablesLockType;
    private HashMap<Integer, Integer> onSites;
    private Instruction currentInstruction;

    /**
     * Creates an Instruction object.  Several data structures are used to keep track of what
     * the transaction is doing.
     * <ul>
     *      <li>variablesLocked - a queue keeping track of the variables that the transaction
     *          has locked</li>
     *      <li>variablesLockType - HashMap which is used to check whether a transaction is trying
     *          to read something it has write locked</li>
     *      <li>onSites - Keeps track of what site the transaction is on</li>
     *      <li>currentInstruction - keep the latest instruction object so that the system knows
     *          what the transaction is doing - mostly for when the transaction is in a queue</li>
     * </ul>
     * @param id - transaction id
     * @param readOnly - whether the transaction is read only
     * @param startTime - Start time of a transaction
     * @param instruction - instruction type
     */
    public Transaction(Integer id, Boolean readOnly, Integer startTime, Instruction instruction) {

        this.id = id;
        this.readOnly = readOnly;
        this.startTime = startTime;
        this.running = true;
        this.variablesLocked = new LinkedList<>();
        this.variablesLockType = new HashMap<>();
        this.dependsOn = new HashMap<>();
        this.dependsOnIt = new HashMap<>();
        this.onSites = initializeOnSites();
        this.currentInstruction = instruction;
    }


    /**
     * Returns the transaction's ID
     * @return Integer - transaction ID
     */
    public Integer getID() {
        return id;
    }


    public void addToDependsOn(String varId, Transaction t) {
        if(dependsOn.get(varId) != null) {
            dependsOn.get(varId).add(t);
        } else {
            LinkedList<Transaction> temp = new LinkedList<>();
            temp.add(t);
            dependsOn.put(varId, temp);
        }
    }


    public void addToDependsOnIt(String varId, Transaction t) {
        if(dependsOnIt.get(varId) != null) {
            dependsOnIt.get(varId).add(t);
        } else {
            LinkedList<Transaction> temp = new LinkedList<>();
            temp.add(t);
            dependsOnIt.put(varId, temp);
        }
    }


    public HashMap<String, LinkedList<Transaction>> getDependsOn() {
        return dependsOn;
    }


    public Boolean checkDependsOn() {
        return (dependsOn.size() > 0);
    }


    public void removeFromDependsOn(String varId) {
        if(dependsOn.get(varId) != null) {
            dependsOn.remove(varId);
        }
    }


    public void removeFromDependsOnIt(String varId) {
        if(dependsOnIt.get(varId) != null) {
            dependsOnIt.remove(varId);
        }
    }

    /**
     * Checks whether transaction is read only
     * @return Boolean - true/false whethe transaction is read only
     */
    public Boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Returns the start time of a transaction
     * @return Integer - start time
     */
    public Integer getStartTime() {
        return startTime;
    }

    /**
     * Adds the current instruction object to the transaction
     * @param instruction - instruction object
     */
    public void addCurrentInstruction(Instruction instruction) {
        currentInstruction = instruction;
    }

    /**
     * Returns the current instruction object
     * @return Instruction - instruction object
     */
    public Instruction getCurrentInstruction() {
        return currentInstruction;
    }

    /**
     * Adds variable ID to locked variables
     * @param varId - string variable ID
     */
    public void addLockedVariable(String varId) {
        variablesLocked.add(varId);
    }

    /**
     * Queue for variables that are locked
     * @return Queue - string queue of variable ids
     */
    public Queue<String> getVariablesLocked() {
        return variablesLocked;
    }


    /**
     * Adds variable and instruction to hashmap
     * @param varId - variable id
     * @param instruction - instruction object
     */
    public void addLockedVariableType(String varId, Instruction instruction) {
        variablesLockType.put(varId, instruction);
    }

    /**
     * Removes a variable from lock variable from Queue
     * @param varId - variable id
     */
    public void removeLockedVariable(String varId) {
        removeLockedVariableType(varId);
        Queue<String> temp = new LinkedList<>();
        while(!variablesLocked.isEmpty()) {
            if(!variablesLocked.peek().equals(varId)) {
                temp.add(variablesLocked.remove());
            } else {
                variablesLocked.remove();
            }
        }
        variablesLocked = temp;
    }

    /**
     * Removes a variable from lock variable type HashMap
     * @param varId - variable id
     */
    public void removeLockedVariableType(String varId) {
        variablesLockType.remove(varId);
    }

    /**
     * Gets lock type info
     * @param varId - variable id
     */
    public Instruction getLockedVariableInfo(String varId) {
        return variablesLockType.get(varId);
    }

    /**
     * Checks lock type info
     * @param varId - variable id
     */
    public String checkLockedVariableType(String varId) {
        if(variablesLockType.get(varId) != null) {
            return variablesLockType.get(varId).getInstruction();
        } else {
            return null;
        }
    }

    /**
     * Changes transaction status to not running
     */
    public void stopTransaction() {
        running = false;
    }

    /**
     * A check to see whether a transaction is still running
     * @return Boolean - true/false whether a transaction is still running
     */
    public Boolean isRunning() {
        return running;
    }

    /**
     * Returns a HashMap of all the sites the transaction is on
     * @return HashMap - HashMap of sites the transaction is on
     */
    public HashMap<Integer, Integer> getOnSites() {
        return onSites;
    }

    /**
     * Initializes the onSite HashMap.  Value is incremented/decremented
     * when a transaction is on/off a site
     * @return HashMap - initializes HashMap with 0s
     */
    private HashMap<Integer, Integer> initializeOnSites() {

        HashMap<Integer, Integer> temp = new HashMap<>();

        for (int i = 1; i <= 10; i++) {
            temp.put(i, 0);
        }
        return temp;
    }

    /**
     * Increments the value in onSites HashMap when a transaction is on
     * a site
     */
    public void plusOnSites(int siteId) {
        onSites.put(siteId, onSites.get(siteId) + 1);
    }

    /**
     * Decrements the value in onSites HashMap when a transaction is off
     * a site
     */
    public void decOnSites(int siteId) {
        onSites.put(siteId, onSites.get(siteId) - 1);
    }

    public HashMap<String, Instruction> getVariablesLockType() {
        return variablesLockType;
    }

    /**
     * toString method for a transaction object
     * @return String - details of a transaction
     */
    public String toString() {
        String result = "\nTransaction: " + this.id + "\n";
        result += "readOnly: " + this.readOnly + "\n";
        result += "startTime: " + this.startTime + "\n";
        return result;
    }
}
