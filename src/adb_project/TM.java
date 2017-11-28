package adb_project;

import java.util.ArrayList;

public class TM {

    private ArrayList<Instruction> instructions;
    private ArrayList<Transaction> transactionsList;
    private static Integer time = 0;

    public TM(ArrayList<Instruction> instructions) {
        this.instructions = instructions;
        this.transactionsList = new ArrayList<>();
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
        Integer variable = null;

        addTransaction(transaction, readOnly, startTime, variable, instruction);
    }
}
