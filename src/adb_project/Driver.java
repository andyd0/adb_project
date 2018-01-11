/**
 * <h1>Replicated Concurrency Control and Recovery</h1>
 * This program implements a distributed database with multiversion
 * concurrency control, deadlock detection, replication and fault
 * recovery.
 * <p>
 * Driver - Takes in the input and starts the Task Manager
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;


import java.io.IOException;
import java.util.ArrayList;


public class Driver {

    public static void main(String[] args) throws IOException {
        String path;
        String file;
        String cwd = System.getProperty("user.dir");
        int inputno = 18;
        Boolean testAll = true;

        if (testAll) {
            for (int i = 1; i < 38; i++) {
                file = "input_" + i + ".txt";
                if (i < 10) {
                    path = cwd + "/tests/" + file;
                } else {
                    path = cwd + "/tests/" + file;
                }

                System.out.println("--------------------------");
                System.out.println("Currently testing: " + file);
                System.out.println("--------------------------");

                Parser parser = new Parser();
                ArrayList<Instruction> instructions = parser.getInstructions(path);

                TM tm = new TM(instructions);
                tm.processInstructions();
            }
        } else {
            if (args.length == 0) {
                System.out.println("No test file specified in args, using path\n");
                System.out.println("--------------------------");
                System.out.println("Currently testing: " + "input_" + inputno + ".txt");
                System.out.println("--------------------------");
                path = cwd + "/tests/input_" + inputno + ".txt";
            } else {
                path = cwd + "/" + args[0];
            }

            Parser parser = new Parser();
            ArrayList<Instruction> instructions = parser.getInstructions(path);

            TM tm = new TM(instructions);
            tm.processInstructions();
        }
    }
}