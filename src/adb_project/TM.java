package adb_project;

import java.util.*;


public class TM {

    private ArrayList<Instruction> instructions;
    private HashMap<Integer, Transaction> transactionsList;
    private static HashMap<String, Queue<Transaction>> lockQueue;
    private static HashMap<Integer, Queue<Transaction>> waitQueue;
    private static Integer time = 0;

    public TM(ArrayList<Instruction> instructions) {
        this.instructions = instructions;
        this.transactionsList = new HashMap<>();
        this.lockQueue = new HashMap<>();
        this.waitQueue = new HashMap<>();
    }

    public void addTransaction(Integer id, Boolean readOnly, Integer startTime,
                               Integer variable, Instruction instruction) {
        Transaction T = new Transaction(id, readOnly, startTime, variable, instruction);
        addToTransactionList(id, T);
    }

    public void addToTransactionList(Integer id, Transaction T) {
        this.transactionsList.put(id, T);
    }

    public Transaction getTransaction(int id) {
        return this.transactionsList.get(id);
    }

    public void setTime() {
        time++;
    }

    public static Integer getTime() {
        return time;
    }

    // Instructions handled by the DM are sent to the DM.  Otherwise,
    // TM handles.
    public void processInstructions() {
        ArrayList<Instruction> instructions = this.instructions;
        DM dm = new DM();

        for(Instruction instruction : instructions) {
            setTime();
            String instruction_type = instruction.getInstruction();
            Integer id;
            Transaction transaction;


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
                    dm.write(transaction, instruction);
                    break;
                case "R":
                    id = instruction.getId();
                    transaction = this.getTransaction(id);

                    if(!transaction.isRunning()) {
                        continue;
                    }

                    transaction.addCurrentInstruction(instruction);
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

    // Creates the transaction
    public void begin(Instruction instruction) {

        Integer startTime = getTime();
        Integer transaction = instruction.getId();
        Boolean readOnly = instruction.getInstruction().contains("RO");

        addTransaction(transaction, readOnly, startTime, null, instruction);
    }

    public static Transaction handleLockQueue(String variable) {
        if(lockQueue.get(variable).size() == 0) {
            return null;
        } else {
            return lockQueue.get(variable).remove();
        }
    }

    public static void addToLockQueue(String variable, Transaction transaction) {
        transaction.setLockTime();
        if(lockQueue.get(variable) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            lockQueue.put(variable, queue);
        } else {
            lockQueue.get(variable).add(transaction);
        }
    }

    public static String peekLockQueue(String variable) {
        if(lockQueue.get(variable).peek() == null) {
            return "N";
        } else {
            return lockQueue.get(variable).peek().getCurrentInstruction().getInstruction();
        }
    }

    public static void addToWaitQueue(Integer siteId, Transaction transaction) {
        if(waitQueue.get(siteId) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            waitQueue.put(siteId, queue);
        } else {
            waitQueue.get(siteId).add(transaction);
        }
    }

    public static Transaction getFromWaitQueue(Integer siteId) {
        if(waitQueue.get(siteId).size() == 0) {
            return null;
        } else {
            return waitQueue.get(siteId).remove();
        }
    }

    public static Boolean emptyLockQueue(String variable) {
        return (lockQueue.get(variable) == null || lockQueue.get(variable).size() == 0);
    }

    public void abort(Integer id) {
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

    public static void abort(Transaction T) {

    }

    private void incrementLockTimes() {

    }
}
