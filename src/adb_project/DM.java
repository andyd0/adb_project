package adb_project;


public class DM {

    public DM() {

    }

    public void handleInstruction(String[] instruction_split) {

        String instruction = instruction_split[0];

        switch (instruction) {
            case "W":
                operations(instruction_split);
                break;
            case "R":
                operations(instruction_split);
                break;
            case "fail":
                fail(instruction_split);
                break;
            case "recover":
                recover(instruction_split);
                break;
        }
    }

    public void operations(String[] instruction_split) {

    }

    public void fail(String[] instruction_split) {
        Integer id = Integer.parseInt(instruction_split[1].replaceAll("\\D+",""));
    }

    public void recover(String[] instruction_split) {
        Integer id = Integer.parseInt(instruction_split[1].replaceAll("\\D+",""));;
    }
}
