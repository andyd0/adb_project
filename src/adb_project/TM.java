package adb_project;

import java.util.ArrayList;

public class TM {

    private ArrayList<String> instructions;
    private ArrayList<Transaction> transactionsList;
    private Integer time = 0;

    public TM(ArrayList<String> instructions) {
        this.instructions = instructions;
        this.transactionsList = new ArrayList<>();
    }

    public void addToTransactionList(Transaction T) {
        this.transactionsList.add(T);
    }

    public void addTransaction(Integer id, Boolean readOnly, Integer startTime, Integer variable, Integer index) {
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

            switch (instruction_split[0]) {
                case "begin":
                    begin(instruction_split);
                    break;
                case "beginRO":
                    begin(instruction_split);
                    break;
                case "end":
                    end(instruction_split);
                default:
                    dm.handleInstruction(instruction_split);
                    break;
            }
       }
    }

    public void begin(String[] instruction_split) {

        Integer startTime = getTime();
        Integer id = Integer.parseInt(instruction_split[1].replaceAll("\\D+",""));
        Boolean readOnly = instruction_split[0].contains("RO");
        Integer variable = -1;
        Integer index = -1;

        addTransaction(id, readOnly, startTime, variable, index);
    }

    public void end(String[] instruction_split) {
        Integer id = Integer.parseInt(instruction_split[1].replaceAll("\\D+",""));;
        this.transactionsList.get(id).stopTransaction();
    }
}
