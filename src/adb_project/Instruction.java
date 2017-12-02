/**
 * <h1>Instruction</h1>
 * Methods and Construction for an instruction
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

public class Instruction {

    private String instruction;
    private Integer id;
    private Integer variable;
    private Integer value = null;

    /**
     * Creates an Instruction object
     * @param instruction - string instruction type
     * @param id - transaction id
     * @param variable - variable id
     * @param value - value of variable
     */
    public Instruction(String instruction, Integer id, Integer variable, Integer value) {
        this.instruction = instruction;
        this.id = id;
        this.variable = variable;
        this.value = value;
    }

    /**
     * Gets the instruction type
     * @return String - instruction type
     */
    public String getInstruction() {
        return instruction;
    }

    /**
     * Gets transaction ID
     * @return Integer - transaction ID
     */
    public Integer getId() {
        return id;
    }

    /**
     * Gets variable ID
     * @return Integer - variable ID
     */
    public Integer getVariable() {
        return variable;
    }

    /**
     * Gets value of variable
     * @return Integer - value of variable
     */
    public Integer getValue() {
        return value;
    }

    /**
     * toString method for instruction object
     * @return String - to string of the fields in the Instruction
     */
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
