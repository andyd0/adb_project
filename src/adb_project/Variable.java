package adb_project;

import java.util.HashMap;

public class Variable {
    private String id;
    private int index;
    private int value;
    private HashMap<Integer, Integer> previousValues;
    private Boolean okToRead;
    private Boolean committed;

    // Each variable xi should be initialized to the value 10i
    // Variable also holds previous value.
    // TODO:  Will this be enough for multiread?
    public Variable(int i) {
        this.id = "x" + Integer.toString(i);
        this.index = i;
        this.value = i * 10;
        this.previousValues = initializePrevValues(i);
        this.committed = false;
        this.okToRead = true;
    }

    private HashMap<Integer, Integer> initializePrevValues(int i) {
        HashMap<Integer, Integer> temp = new HashMap<>();
        temp.put(1, i * 10);
        return temp;
    }

    public String getId() {
        return this.id;
    }

    public int getValue() {
        return this.value;
    }

    public int getPreviousValue(Integer time) {

        HashMap<Integer, Integer> temp = this.previousValues;
        Integer value = -1;

        for (HashMap.Entry<Integer, Integer> entry : temp.entrySet()) {
            if (time > entry.getKey()) {
                value = entry.getValue();
            }
        }
        return value;
    }

    public int getIndex() {
        return this.index;
    }

    public void updateValue(int val, int time) {
        this.previousValues.put(time, val);
        this.value = val;
    }

    public void setOkToRead(Boolean okNotOk) {
        okToRead = okNotOk;
    }

    public Boolean getOkToRead() {
        return okToRead;
    }

    public void valueCommitted() {
        committed = true;
    }

    public Boolean checkCommitted() {
        return committed;
    }

    public String toString() {
        String result = "";
        result += "\nid: " + this.id +"\n";
        result += String.format("index: %1d\n", this.index);
        result += String.format("data: %1d\n", this.value);
        return result;
    }
}
