package adb_project;

import java.util.HashMap;

public class Site {

    private int number;

    // List of variables stored on this site
    // Boolean indicates is variable is available on this site or not
    // Evens at all sites, Odds only at (1 + index number mod 10)th site
    private HashMap<Variable, Boolean> variables;

    // whether the site is running
    private boolean isUp;

    public Site(int i) {
      this.number = i;
      this.variables = new HashMap<Variable, Boolean>();
    }

    public String toString() {
        String result = "";
        result += "This is site: " + this.number +"\n";
        return result;
    }

    /* 
      Need to think about locktable since EACH site will keep track of 
      EACH active transaction and will maintain locks on EACH variable that
      the transaction accesses

      So probably a multi-level mapping:
      Site -> multiple Transactions -> multiple Variables
    */

    /*
    public void LockTable(Transaction T) {
      lockTable.put(T.getID(), T.getVariable());
    }
    */
}
