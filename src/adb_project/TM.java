package adb_project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class TM {

    private ArrayList<Instruction> instructions;
    private ArrayList<Transaction> transactionsList;
    private static HashMap<String, Queue<Transaction>> lockQueue;
    private static Integer time = 0;

    public TM(ArrayList<Instruction> instructions) {
        this.instructions = instructions;
        this.transactionsList = new ArrayList<>();
        this.lockQueue = new HashMap<>();
    }

    public void addToTransactionList(Transaction T) {
        this.transactionsList.add(T);
    }

    public void addTransaction(Integer id, Boolean readOnly, Integer startTime,
                               Integer variable, Instruction instruction) {
        Transaction T = new Transaction(id, readOnly, startTime, variable, instruction);
        addToTransactionList(T);
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
            Integer variable;
            Transaction transaction;

            switch (instruction_type) {
                case "begin":
                    begin(instruction);
                    break;
                case "beginRO":
                    begin(instruction);
                    break;
                case "W":
                    id = instruction.getID();
                    transaction = this.getTransaction(id - 1);
                    transaction.addCurrentInstruction(instruction);
                    dm.write(transaction, instruction);
                    break;
                case "R":
                    id = instruction.getID();
                    transaction = this.getTransaction(id - 1);
                    transaction.addCurrentInstruction(instruction);
                    dm.read(transaction, instruction);
                    break;
                case "fail":
                    dm.fail(instruction);
                    break;
                case "recover":
                    dm.recover(instruction);
                    break;
                case "dump":
                    dm.dump(instruction);
                    break;
                case "end":
                    id = instruction.getID();
                    dm.end(this.getTransaction(id - 1));
                    break;
            }
        }
    }

    // Creates the transaction
    public void begin(Instruction instruction) {

        Integer startTime = getTime();
        Integer transaction = instruction.getID();
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
        if(lockQueue.get(variable) == null) {
            Queue<Transaction> queue = new LinkedList<>();
            queue.add(transaction);
            lockQueue.put(variable, queue);
        } else {
            lockQueue.get(variable).add(transaction);
        }
    }

    public static Boolean emptyLockQueue(String variable) {
        return (lockQueue.get(variable) == null || lockQueue.get(variable).size() == 0);
    }

}
