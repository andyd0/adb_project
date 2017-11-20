package adb_project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Parser {

    public String[] parseInstruction (String instruction)  {
        return instruction.split("[\\(\\,\\,\\)]");
    }

    public ArrayList<String> getInstructions() throws IOException {

        String cwd = System.getProperty("user.dir");

        BufferedReader br = new BufferedReader(new FileReader(cwd + "/input/input_01.txt"));

        ArrayList<String> instructions = new ArrayList<>();
        String instruction;

        while (br.ready()) {
            instruction = br.readLine();
            instructions.add(instruction);
        }

        return instructions;

    }
}
