package adb_project;

import java.util.ArrayList;
import java.util.HashMap;

public class Site {
    private int number;
    private ArrayList<Variable> variables;

    /*
      Java doesn't have tuple types so using a hashmap to emulate it:
      This is a hashmap with Variable as the key and it's value is
      a tuple-like record created using a single record hashmap.


      This looks like [Variable -> [TransactionID, WriteLock]]
    */
    private HashMap<Variable, HashMap<Transaction, String>> lockTable;
    private boolean isRunning;

    public Site(int num) {
        this.number = num;
        this.isRunning = true;
        this.variables = new ArrayList<Variable>();
        this.lockTable = new HashMap<Variable, HashMap<Transaction, String>>();

        for (int i=1; i<=20; i++) {
            Variable v = new Variable(i);

            if (i%2 == 0) {
                variables.add(v);
            } else if ((1+i%10) == num) {
                variables.add(v);
            }
            lockTable.put(v, new HashMap<Transaction, String>());
        }
    }

    public String toString() {
        String result = "";
        result += "This is site: " + number +"\n";
        return result;
    }

    // check if site is up and running
    public Boolean isRunning() {
        return this.isRunning;
    }

    public void fail() {

    }

    public void recover() {

    }

    public int getSiteNum() {
        return this.number;
    }

    // check if variable exists on this site by supplying variable object
    public Boolean hasVariable(Variable x) {
        for (Variable v: variables) {
            if (x.getId().equals(v.getId())) {
                return true;
            }
        }
        return false;
    }

    // check if variable exists on this site by supplying variable id
    public Boolean hasVariable(String id) {
        for (Variable v: variables) {
            if (id.equals(v.getId())) {
                return true;
            }
        }
        return false;
    }

    // get all variables on this site
    public ArrayList<Variable> getAllVariables() {
        return this.variables;
    }

    // get specific variable by supplying its id
    public Variable getVariable(String id) {
        for (Variable v: variables) {
            if (id.equals(v.getId())) {
                return v;
            }
        }
        return null;
    }

    // get variable's data by supplying its id
    public Integer getVariableData(String id) {
        Integer res = 0;
        for (Variable v: variables) {
            if (id.equals(v.getId())) {
                res = v.getData();
            }
        }
        return res;
    }

    public boolean lockVariable(Transaction t, String varId, String lockType) {
        for (Variable v: lockTable.keySet()) {
            if (varId.equals(v.getId())) {
                System.out.println("Locking variable: " + varId + " for Transaction: " + t.getID() + " with lock: " + lockType);
                lockTable.get(v).put(t, lockType);
                return true;
            }
        }
        return false;
    }
}
