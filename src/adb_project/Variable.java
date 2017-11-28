package adb_project;

public class Variable {
    private String id;
    private int index;
    private int data;
    private int previousValue;

    // Each variable xi should be initialized to the value 10i
    // Variable also holds previous value.
    // TODO:  Will this be enough for multiread?
    public Variable(int i) {
        this.id = "x" + Integer.toString(i);
        this.index = i;
        this.data = i * 10;
        this.previousValue = i * 10;
    }

    public String getId() {
        return this.id;
    }

    public int getData() {
        return this.data;
    }

    public int getPreviousValue() {
        return this.previousValue;
    }

    public int getIndex() {
        return this.index;
    }

    public void updateData(int val) {
        this.previousValue = this.data;
        this.data = val;
    }

    public String toString() {
        String result = "";
        result += "\nid: " + this.id +"\n";
        result += String.format("index: %1d\n", this.index);
        result += String.format("data: %1d\n", this.data);
        return result;
    }
}
