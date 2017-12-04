/**
 * <h1>TM</h1>
 * TM Constructor and its methods.  Handles the task manager tasks.
 * Main role is creating the tasks and passing the instructions to the
 * data manager.
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;


public class TM {

    private ArrayList<Instruction> instructions;
    private HashMap<Integer, Transaction> transactionsList;
    private static HashMap<String, Queue<Transaction>> lockQueue;
    private static HashMap<Integer, Queue<Transaction>> waitQueue;
    private static Integer time = 0;

    /**
     * Creates a Task Manager Object.  Holds the instructions,
     * transaction list, variable lock queues and site wait queues
     * @param instructions - An ArrayList of instruction objects
     */
    public TM(ArrayList<Instruction> instructions) {
        this.instructions = instructions;
        this.transactionsList = new HashMap<>();
        this.lockQueue = new HashMap<>();
        this.waitQueue = new HashMap<>();
    }

    /**
     * Creates a new transaction and adds it to a HashMap
     * @param id - id of transaction
     * @param readOnly - whether the transaction is read only
     * @param startTime - when the transaction was created
     * @param instruction - instruction object for transaction
     */
    private void addTransaction(Integer id, Boolean readOnly, Integer startTime, Instruction instruction) {
        Transaction T = new Transaction(id, readOnly, startTime, instruction);
        addToTransactionList(id, T);
    }


    /**
     * Adds a transaction to a HashMap of transactions
     * @param id - id of transaction
     * @param T - a transaction
     */
    private void addToTransactionList(Integer id, Transaction T) {
        this.transactionsList.put(id, T);
    }

    /**
     * Returns a transaction object
     * @param id - id of transaction
     * @return T - a transaction object
     */
    private Transaction getTransaction(int id) {
        return this.transactionsList.get(id);
    }

    /**
     * Increments the time during processing
     */
    private void setTime() {
        time++;
    }

    /**
     * Gets the current time during processing
     * @return Integer - time
     */
    public static Integer getTime() {
        return time;
    }

    /**
     * Takes each instruction and hands off to the Data Manager where
     * applicable otherwise executed by the Task Manager
     */
    public void processInstructions() {
        ArrayList<Instruction> instructions = this.instructions;
        DM dm = new DM();

        for(Instruction instruction : instructions) {
            setTime();
            String instruction_type = instruction.getInstruction();
            Integer id;
            Transaction transaction;

            // Since the instruction list may have instructions for terminated
            // transactions, a check is added to continue if one is found
            switch (instruction_type) {
                case "begin":
                    begin(instruction);
                    break;
                case "beginRO":
                    begin(instruction);
                    break;
                case "W":
                    id = instruction.getId();
                    transaction = this.getTransaction(id);

                    if(!transaction.isRunning()) {
                        continue;
                    }

                    transaction.addCurrentInstruction(instruction);

                    if(!dm.deadLockCheck(transaction, instruction))
                        dm.write(transaction, instruction);

                    break;
                case "R":
                    id = instruction.getId();
                    transaction = this.getTransaction(id);

                    if(!transaction.isRunning()) {
                        continue;
                    }

                    transaction.addCurrentInstruction(instruction);

                    if(!transaction.isReadOnly() && !dm.deadLockCheck(transaction, instruction))
                        dm.read(transaction, instruction);
                    else
                        dm.read(transaction, instruction);

                    break;
                case "fail":
                    id = instruction.getId();
                    abort(id);
                    dm.fail(instruction);
                    break;
                case "recover":
                    dm.recover(instruction);
                    break;
                case "dump":
                    dm.dump();
                    break;
                case "dumpx":
                    dm.dump("x" + instruction.getValue().toString());
                    break;
                case "dumpi":
                    dm.dump(instruction.getValue());
                    break;
                case "end":
                    id = instruction.getId();
                    transaction = this.getTransaction(id);

                    if(!transaction.isRunning()) {
                        continue;
                    }

                    dm.end(this.getTransaction(id));
                    break;
            }
            incrementLockTimes();
        }
    }

    /**
     * Sets up a transaction
     * @param instruction - an instruction object
     */
    public void begin(Instruction instruction) {

        Integer startTime = getTime();
        Integer transaction = instruction.getId();
        Boolean readOnly = instruction.getInstruction().contains("RO");

        addTransaction(transaction, readOnly, startTime, instruction);
    }

    /**
     * Removes a transaction from a variable lock queue and returns
     * a transaction object
     * @param variableId - variable id
     * @return T - transaction object if there is one on queue
     */
    public static Transaction handleLockQueue(String variableId) {
        if(lockQueue.get(variableId).size() == 0) {
            return null;
        } else {
            return lockQueue.get(variableId).remove();
        }
    }

    /**
     * Adds a a transaction to a variable lock queue
     * a transaction object
     * @param variableId - variable id
     * @param transaction - transaction object
     */
    public static void addToLockQueue(String variableId, Transaction transaction) {
        transaction.setLockTime();
        if(lockQueue.get(variableId) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            lockQueue.put(variableId, queue);
        } else {
            lockQueue.get(variableId).add(transaction);
        }
    }

    /**
     * Gets the instruction at the start of the start of the queue.  This is to
     * handle cases where an unlock a variable may have more than one transaction
     * waiting to read lock it
     * @param variableId - variable id
     * @return String - instruction type
     */
    public static String peekLockQueue(String variableId) {
        if(lockQueue.get(variableId).peek() == null) {
            return "N";
        } else {
            return lockQueue.get(variableId).peek().getCurrentInstruction().getInstruction();
        }
    }

    /**
     * Adds a transaction to a sites wait queue when the site is unavailable
     * @param siteId - site ID
     * @param transaction - a transaction object
     */
    public static void addToWaitQueue(Integer siteId, Transaction transaction) {
        if(waitQueue.get(siteId) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            waitQueue.put(siteId, queue);
        } else {
            waitQueue.get(siteId).add(transaction);
        }
    }

    /**
     * Gets a transaction from the wait queue if there is one waiting
     * @param siteId - site ID
     * @return Transaction - a transaction object
     */
    public static Transaction getFromWaitQueue(Integer siteId) {
        if(waitQueue.get(siteId).size() == 0) {
            return null;
        } else {
            return waitQueue.get(siteId).remove();
        }
    }

    /**
     * Checks to see if a variable's lock queue is empty
     * @param variableId - string id of a variable
     * @return Boolean - true/false whether the queue is empty
     */
    public static Boolean emptyLockQueue(String variableId) {
        return (lockQueue.get(variableId) == null || lockQueue.get(variableId).size() == 0);
    }

    /**
     * Aborts a transaction if terminated for some reason - either because of deadlock or site
     * failure
     * @param id - transaction id
     */
    private void abort(Integer id) {
        abort(transactionsList.get(id));
        for (HashMap.Entry<Integer, Transaction> entry : this.transactionsList.entrySet())
        {
            HashMap<Integer, Integer> sites = entry.getValue().getOnSites();
            if(sites != null) {
                if(sites.get(id) != null && sites.size() > 0 && sites.get(id) > 0) {
                    entry.getValue().stopTransaction();
                    System.out.println("Transaction " + entry.getValue().getID() + " aborted because Site " +
                                        id.toString() + " has failed");
                }
            }
        }
    }

    //private static HashMap<String, Queue<Transaction>> lockQueue;
    //private static HashMap<Integer, Queue<Transaction>> waitQueue;
    public static void abort(Transaction T) {

        // Remove from variable lock queue
        for(HashMap.Entry<String, Queue<Transaction>> locks : lockQueue.entrySet()) {
            Queue<Transaction> newQueue = new LinkedList<>();
            Queue<Transaction> oldQueue = locks.getValue();

            while(!oldQueue.isEmpty()) {
                Transaction queueT = oldQueue.remove();
                if(queueT != T) {
                    newQueue.add(queueT);
                }
            }
            lockQueue.put(locks.getKey(), newQueue);
        }

        // Remove from site wait queue
        for(HashMap.Entry<Integer, Queue<Transaction>> locks : waitQueue.entrySet()) {
            Queue<Transaction> newQueue = new LinkedList<>();
            Queue<Transaction> oldQueue = locks.getValue();

            while(!oldQueue.isEmpty()) {
                Transaction queueT = oldQueue.remove();
                if(queueT != T) {
                    newQueue.add(queueT);
                }
            }
            waitQueue.put(locks.getKey(), newQueue);
        }
    }

    private void incrementLockTimes() {

    }
}
