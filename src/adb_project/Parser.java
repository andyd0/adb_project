package adb_project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Parser {

    // Gets the number from a string
    public Integer parseInt(String T) {
        return Integer.parseInt(T.replaceAll("\\D+",""));
    }

    // Breaks up an input line
    private String[] parseInstruction (String instruction)  {
        return instruction.split("[\\(\\,\\,\\)]");
    }

    // Reads in the input
    public ArrayList<Instruction> getInstructions(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));

        ArrayList<Instruction> instructions = new ArrayList<>();
        String line;

        while (br.ready()) {
            line = br.readLine();
            String[] temp = parseInstruction(line);
            Instruction instruction;

            if(temp[0].equals("R")) {
                instruction = new Instruction(temp[0], parseInt(temp[1]), parseInt(temp[2]), null);
            } else if(temp[0].equals("W")) {
                instruction = new Instruction(temp[0], parseInt(temp[1]), parseInt(temp[2]), Integer.parseInt(temp[3]));
            } else if(temp.length == 1) {
                instruction = new Instruction(temp[0], null, null, null);
            } else {
                instruction = new Instruction(temp[0], parseInt(temp[1]), null, null);
            }

            instructions.add(instruction);
        }

        return instructions;
    }

}
