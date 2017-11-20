package adb_project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Parser {

    public String[] parseInstruction (String instruction)  {
        return instruction.split("[\\(\\,\\,\\)]");
    }

    public ArrayList<String> getInstructions(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));

        ArrayList<String> instructions = new ArrayList<>();
        String instruction;

        while (br.ready()) {
            instruction = br.readLine();
            instructions.add(instruction);
            System.out.println("instruction: " + instruction);
        }

        return instructions;
    }
}
