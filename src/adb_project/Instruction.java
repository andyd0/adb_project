package adb_project;

// So that parsing can be handled completely with the parser and simplify
// passing around of instruction information, a class was created with the
// required fields.

public class Instruction {

    private String instruction;
    private Integer id;
    private Integer variable;
    private Integer value = null;

    public Instruction(String instruction, Integer id, Integer variable, Integer value) {
        this.instruction = instruction;
        this.id = id;
        this.variable = variable;
        this.value = value;
    }

    public String getInstruction() {
        return instruction;
    }

    public Integer getId() {
        return id;
    }

    public Integer getVariable() {
        return variable;
    }

    public Integer getValue() {
        return value;
    }

    public String toString() {
        String result = "------\n";
        result += "\ninstruction: " + this.instruction;
        result += "\nid: " + this.id;
        result += "\nvariable: " + this.variable;
        result += "\nvalue: " + this.value;
        result += "\n------";
        return result;
    }
}
