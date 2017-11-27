package adb_project;

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

    public Integer getID() {
        return id;
    }

    public Integer getVariable() {
        return variable;
    }

    public Integer getValue() {
        return value;
    }
}
