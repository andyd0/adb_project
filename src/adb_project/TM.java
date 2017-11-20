package adb_project;

import java.util.ArrayList;

public class TM {

    private ArrayList<String> instructions;
    private ArrayList<Transaction> transactionsList;
    private Integer time = 0;

    public TM(ArrayList<String> instructions) {
        this.instructions = instructions;
        this.transactionsList = new ArrayList<>();
        this.time = time;
    }

    public void addToTransactionList(Transaction T) {
        this.transactionsList.add(T);
    }

    public void addTransaction(String[] tFields) {

        Integer id = Integer.parseInt(tFields[0]);
        Boolean readOnly = Boolean.getBoolean(tFields[1]);
        Integer startTime = Integer.parseInt(tFields[2]);
        Integer variable = Integer.parseInt(tFields[3]);
        Integer index = Integer.parseInt(tFields[4]);

        Transaction T = new Transaction(id, readOnly, startTime, variable, index);
        addToTransactionList(T);
    }

    public void setTime() {
        this.time++;
    }

    public Integer getTime() {
        return this.time;
    }

    public void startProcessing() {
        ArrayList<String> instructions = this.instructions;
        Parser parser = new Parser();
        DM dm = new DM();

        for(String instruction : instructions) {
            setTime();
            String[] instruction_split = parser.parseInstruction(instruction);
            dm.handleInstruction(instruction_split);
        }
    }

}
