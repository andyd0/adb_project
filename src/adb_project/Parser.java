/**
 * <h1>Parser</h1>
 * Handles parsing the instructions taken from input and
 * creates an Instruction object.
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */


package adb_project;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class Parser {


    /**
     * Gets the number from a string.  Mostly to handle the x#
     * @param getInt - string with concatenated number
     * @return An integer.
     */
    private Integer parseInt(String getInt) {
        return Integer.parseInt(getInt.replaceAll("\\D+",""));
    }


    /**
     * Breaks up an input line into an array of strings
     * @param instruction - string input that has paranthesis and commas separating
     *                      required fields
     * @return String array.
     */
    private String[] parseInstruction (String instruction)  {
        return instruction.split("[\\(\\,\\,\\)]");
    }


    /**
     * Breaks up an input line into an array of strings
     * @param path - path to file with instructions on each line
     * @return An ArrayList of Instruction Objects.
     */
    public ArrayList<Instruction> getInstructions(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));

        ArrayList<Instruction> instructions = new ArrayList<>();
        String line;

        while (br.ready()) {
            line = br.readLine();
            String[] temp = parseInstruction(line);
            Instruction instruction;

            for(int i = 0; i < temp.length; i++) {
                temp[i] = temp[i].trim();
            }

            if(temp[0].equals("R")) {
                instruction = new Instruction(temp[0], parseInt(temp[1]), parseInt(temp[2]), null);
            } else if(temp[0].equals("W")) {
                instruction = new Instruction(temp[0], parseInt(temp[1]), parseInt(temp[2]), Integer.parseInt(temp[3]));
            } else if(temp[0].equals("dump") && temp.length == 1) {
                instruction = new Instruction(temp[0], null, null, null);
            } else if(temp[0].equals("dump") && temp[1].charAt(0) == 'x') {
                instruction = new Instruction(temp[0]+"x", null, null, parseInt(temp[1]));
            } else if(temp[0].equals("dump") && temp.length > 1) {
                instruction = new Instruction(temp[0]+"i", null, null, Integer.parseInt(temp[1]));
            } else {
                instruction = new Instruction(temp[0], parseInt(temp[1]), null, null);
            }

            instructions.add(instruction);
        }

        return instructions;
    }

}
