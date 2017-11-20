package adb_project;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class Driver {
    public static void main(String[] args) throws IOException {
      /*

      // DO NOT REMOVE
      // Need this validation for passing files through command line

      if (args.length == 0) {
        System.out.println("Please specify an input file. eg. java Driver"
          + " /path/to/<test file>\n");

        System.out.println("If you're running from the terminal: "
          + " cd /src/ \n"
          + "java -classpath . adb_project.Driver <test file>\n");

        System.exit(0);
      }

      // String path = args[0];
      // Call Parser here with path (ie. filename)

      */

      Parser parser = new Parser();
      ArrayList<String> instructions = parser.getInstructions();

      TM tm = new TM(instructions);
      tm.startProcessing();

      // Sample code to create 5 transactions:
      System.out.println("------------ Transactions ------------");
      ArrayList<Transaction> transactions = new ArrayList<>();
      for (int i=1; i<=10; i++) {
        Transaction t = new Transaction(i, false, i, i, i);
        System.out.println(t);
      }

      /*
        TODO: This needs to be moved to the Site() constructor
        since each site should be initialized with all the vars
        Sample code creating 20 variables:
      */
      System.out.println("------------ Variables ------------");
      for (int i=1; i<=20; i++) {
        Variable v = new Variable(i);
        System.out.println(v);
      }

      System.out.println("------------ Sites ------------\n");
      // Sample code creating 10 sites
      ArrayList<Site> sites = new ArrayList<>();
      for (int i=1; i<=10; i++) {
        Site s = new Site(i);
        System.out.println(s);
      }
    }
}

// command to run from shell: `java -classpath . adb_project.Driver input.txt`
