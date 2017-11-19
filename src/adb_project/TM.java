package adb_project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class TM {

    public void transctionList() {

    }

    public ArrayList<String> parseInput () throws IOException {

        String cwd = System.getProperty("user.dir");

        BufferedReader br = new BufferedReader(new FileReader(cwd + "/input/input_01.txt"));

        ArrayList<String> lines = new ArrayList<>();
        String line;

        while (br.ready()) {
            line = br.readLine();
            lines.add(line);
        }

        for(String input : lines) {
            String[] test = input.split("[\\(\\,\\,\\)]");
        }

        return lines;
    }
}
