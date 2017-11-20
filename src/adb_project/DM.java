package adb_project;


public class DM {

    public void handleInstruction(String[] instruction_split) {

        String instruction = instruction_split[0];

        switch (instruction) {
            case "begin":
                begin(instruction_split);
            case "W":
                operations(instruction_split);
            case "R":
                operations(instruction_split);
            case "end":
                end(instruction_split);
            case "fail":
                fail(instruction_split);
            case "recover":
                recover(instruction_split);
            case "dump":
                dump(instruction_split);
        }
    }

    public void begin(String[] instruction_split) {
        
    }

    public void end(String[] instruction_split) {

    }

    public void operations(String[] instruction_split) {

    }

    public void fail(String[] instruction_split) {

    }

    public void recover(String[] instruction_split) {

    }

    public void dump(String[] instruction_split) {

    }
}
