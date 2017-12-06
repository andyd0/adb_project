/**
 * <h1>Variable</h1>
 * Methods and Construction for a variable object
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

import java.util.HashMap;

public class Variable {
    private String id;
    private int value;
    private HashMap<Integer, Integer> previousValues;
    private Boolean okToRead;
    private Boolean committed;

    /**
     * Variable Contructor. okToRead is a check during site fail/recover.
     * previousValues keeps track of the updated values of the variable
     * by time.
     * @param i - variable id
     */
    public Variable(int i) {
        this.id = "x" + Integer.toString(i);
        this.value = i * 10;
        this.previousValues = initializePrevValues(i);
        this.committed = false;
        this.okToRead = true;
    }

    /**
     * Initializese the HashMap that keeps track of previous values
     * of the variable
     * @return HashMap
     */
    private HashMap<Integer, Integer> initializePrevValues(int i) {
        HashMap<Integer, Integer> temp = new HashMap<>();
        temp.put(1, i * 10);
        return temp;
    }

    /**
     * Gets the current value of a variable
     * @return int - current value of variable
     */
    public int getValue() {
        return this.value;
    }

    /**
     * Gets the previous value of a variable based on time
     * @param time - when it was updated
     */
    public int getPreviousValue(Integer time) {

        HashMap<Integer, Integer> temp = this.previousValues;
        Integer value = -1;

        for (HashMap.Entry<Integer, Integer> entry : temp.entrySet()) {
            if (time >= entry.getKey()) {
                value = entry.getValue();
            }
        }
        return value;
    }

    /**
     * Updates a variable with new value while storing
     * it in previous values with time
     * @param value - updated value
     * @param time - when it was updated
     */
    public void updateValue(int value, int time) {
        this.previousValues.put(time, value);
        this.value = value;
    }

    /**
     * Sets whether it's ok to read a variable after
     * a site failure - just for even variables
     * @param okNotOk
     */
    public void setOkToRead(Boolean okNotOk) {
        okToRead = okNotOk;
    }

    /**
     * Checks whether it's ok to read a variable after
     * a site failure - just for even variables
     */
    public Boolean getOkToRead() {
        return okToRead;
    }

    /**
     * Sets whether a value has been committed
     */
    public void valueCommitted() {
        committed = true;
    }

    /**
     * Checks whether the variable has been committed
     * @return Boolean
     */
    public Boolean checkCommitted() {
        return committed;
    }

    /**
     * toString method for a variable object
     * @return String - details of a variable
     */
    public String toString() {
        String result = "";
        result += "\nid: " + this.id +"\n";
        result += String.format("data: %1d\n", this.value);
        return result;
    }
}
