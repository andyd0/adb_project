package adb_project;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;


public class Driver {

    public static void main(String[] args) throws IOException {

        Parser parser = new Parser();
        ArrayList<String> instructions = parser.getInstructions();

        TM tm = new TM(instructions);
        tm.startProcessing();
    }
}
