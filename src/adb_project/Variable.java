package adb_project;

public class Variable {
    private String id;
    private int index;
    private int data;

    // Each variable xi should be initialized to the value 10i
    public Variable(int i) {
        this.id = "x" + Integer.toString(i);
        this.index = i;
        this.data = i * 10;
    }

    public String getID() {
        return this.id;
    }

    public int getData() {
        return this.data;
    }

    public int getIndex() {
        return this.index;
    }

    public void updateData(int val) {
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
