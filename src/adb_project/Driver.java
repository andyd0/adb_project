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
        String cwd = System.getProperty("user.dir");

        if (args.length == 0) {
            System.out.println("No test file specified in args, using path\n");
            path = cwd + "/tests/input_20.txt";
        } else {
            path = cwd + "/" + args[0];
        }

        Parser parser = new Parser();
        ArrayList<Instruction> instructions = parser.getInstructions(path);

        TM tm = new TM(instructions);
        tm.processInstructions();
    }
}

// command to run from shell: `java -classpath . adb_project.Driver input.txt`
// test for slack