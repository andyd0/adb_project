package adb_project;

import java.util.HashMap;

public class Site {

    private int number;
    private HashMap<Variable, Boolean> variables;
    private boolean isUp;

    public Site(int num) {
        this.number = num;
        this.isUp = true;
        this.variables = new HashMap<Variable, Boolean>();

        for (int i=1; i<=20; i++) {
            Variable v = new Variable(i);

            if (i%2 == 0) {
                variables.put(v, true);
            } else if ((1+i%10) == num) {
                variables.put(v, true);
            } else {
                variables.put(v, false);
            }
        }
    }

    public String toString() {
        String result = "";
        result += "This is site: " + number +"\n";
        return result;
    }

    public Boolean hasVariable(Variable x) {
        for (Variable v: variables.keySet()){
            if (v.getId().equals(x.getId())) {
                Boolean exists = variables.get(v);
                if (exists) {
                  return true;
                }
            }
        }
        return false;
    }

    public Boolean isRunning() {
      return isUp;
    }
}
