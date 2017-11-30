package adb_project;

import java.io.IOException;
import java.util.ArrayList;

public class Driver {
    public static void main(String[] args) throws IOException {
      String path;
      String cwd = System.getProperty("user.dir");

      if (args.length == 0) {
        System.out.println("No test file specified in args, using path\n");
        path = cwd + "/tests/input_16.txt";
      } else {
        path = cwd + "/" + args[0];
      }

      Parser parser = new Parser();
      ArrayList<Instruction> instructions = parser.getInstructions(path);

      TM tm = new TM(instructions);
      tm.processInstructions();

//      // Sample code to create 5 transactions:
//      System.out.println("------------ Transactions ------------");
//      ArrayList<Transaction> transactions = new ArrayList<>();
//      for (int i=1; i<=10; i++) {
//        Transaction t = new Transaction(i, false, i, i, i);
//        System.out.println(t);
//      }
//
//      /*
//        TODO: This needs to be moved to the Site() constructor
//        since each site should be initialized with all the vars
//        Sample code creating 20 variables:
//      */
//      System.out.println("------------ Variables ------------");
//      for (int i=1; i<=20; i++) {
//        Variable v = new Variable(i);
//        System.out.println(v);
//      }
//
//      System.out.println("------------ Sites ------------\n");
//      // Sample code creating 10 sites
//      ArrayList<Site> sites = new ArrayList<>();
//      for (int i=1; i<=10; i++) {
//        Site s = new Site(i);
//        System.out.println(s);
//      }
    }
}

// command to run from shell: `java -classpath . adb_project.Driver input.txt`
// test for slack