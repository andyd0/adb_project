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

    public Transaction getTransaction(int id) {
        return this.transactionsList.get(id);
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
            String instruction_type = instruction_split[0];
            Integer id;
            Integer index;

            switch (instruction_split[0]) {
                case "begin":
                    dm.addToHistory(this.time);
                    id = ParseInt(instruction_split[1]);
                    begin(instruction_type, id);
                    break;
                case "beginRO":
                    dm.addToHistory(this.time);
                    id = ParseInt(instruction_split[1]);
                    begin(instruction_type, id);
                    break;
                case "W":
                    id = ParseInt(instruction_split[1]);
                    index = ParseInt(instruction_split[2]);
                    Integer value = Integer.parseInt(instruction_split[3]);
                    dm.write(this.getTransaction(id - 1), index, value, this.getTime());
                    break;
                case "R":
                    id = ParseInt(instruction_split[1]);
                    index = ParseInt(instruction_split[2]);
                    dm.read(this.getTransaction(id - 1), index, this.getTime());
                    break;
                case "fail":
                    dm.addToHistory(this.time);
                    dm.fail(Integer.parseInt(instruction_split[1]));
                    break;
                case "recover":
                    dm.addToHistory(this.time);
                    dm.recover(Integer.parseInt(instruction_split[1]));
                    break;
                case "dump":
                    dm.dump(instruction_split);
                    break;
                case "end":
                    dm.addToHistory(this.time);
                    id = ParseInt(instruction_split[1]);
                    end(id);
                    break;
            }
       }
    }

    public void begin(String instruction, Integer id) {

        Integer startTime = getTime();
        Boolean readOnly = instruction.contains("RO");
        Integer variable = -1;
        Integer index = -1;

        addTransaction(id, readOnly, startTime, variable, index);
    }

    public void end(Integer id) {
        this.transactionsList.get(id - 1).stopTransaction();
    }

    private Integer ParseInt(String T) {
        return Integer.parseInt(T.replaceAll("\\D+",""));
    }

}
