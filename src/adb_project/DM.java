/**
 * <h1>DM</h1>
 * DM Contructor and its methods.  Handles the data manager tasks.
 * Main role is to execute transaction instructions on variables and
 * to manage sites.  DM acts a super data manager.
 *
 * @author  Andres Davila
 * @author  Pranay Pareek
 * @since   07-12-2017
 */

package adb_project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import javafx.util.Pair;


public class DM {

    private final Integer MAX_SITES = 10;
    private int failedSiteCount;
    private ArrayList<Site> sites;
    private HashMap<String, ArrayList<LinkedList<Pair>>> variableTracker = new HashMap<>();
    private Set<Transaction> transactionSet = new HashSet<>();

    /**
     * Creates a Data Manager Object.  Keeps tracks of all sites
     * and a count of how many sites have failed
     */
    public DM() {
        failedSiteCount = 0;
        sites = initializeSites();
    }

    /**
     * Initializes the sites array list
     * @return ArrayList - ArrayList of sites
     */
    private ArrayList<Site> initializeSites() {
        ArrayList<Site> sites = new ArrayList<>();
        for (int i = 1; i <= MAX_SITES; i++) {
            Site s = new Site(i);
            sites.add(s);
        }
        return sites;
    }

    /**
     * Handles all write operations
     * Checks to see whether the write is to all sites or specific site for odd
     * variables. Each have their checks to how to handle the write.
     *
     * <ul>
     *      <ol>First the sites are checked to see failed and / or locked.  This check is used
     *      to allow the transaction to write if there are any sites not in fail state or locked
     *      variables</ol>
     *      <ol>If cleared, transaction gets its locks</ol>
     *      <ol>If the check doesn't clear, then deadlock is checked</ol>
     *      <ol>If neither of the two, then the site must be in a fail state so add to
     *          site wait queue</ol>
     * </ul>
     *
     * @param T - Transaction Object
     * @param instruction - Instruction Object
     */
    public void write(Transaction T, Instruction instruction) {

        String transactionID = "T" + T.getID().toString();

        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();
        Integer value = instruction.getValue();

        // Getting a count of failed sites for later checking against what is
        // not locked
        if(index % 2 == 0) {

            Integer failCheck = MAX_SITES - getFailCount();

            // Counts up the number locked

            // If the number of variables that are not locked is greater than or equal to the
            // failCheck then we can lock
            int not_locked = 0;
            for(Site site: sites){
                not_locked += (!site.getSiteState().equals("failed") && !site.isVariableLocked(variable)) ? 1 : 0;
            }

            if(not_locked >= failCheck) {
                for (Site site : sites) {
                    if (!site.getSiteState().equals("failed")) {
                        site.lockVariable(T, variable, instruction);
                    }
                }

                T.addLockedVariable(variable);
                T.addLockedVariableType(variable, instruction);

                System.out.println(transactionID + " wrote to " + variable + " to all sites " + ": " + value.toString());

            } else {
                TM.addToLockQueue(variable, T);
            }

        } else {

            Integer siteId = 1 + index % 10;
            Site site = sites.get(siteId - 1);

            if(!site.getSiteState().equals("failed")) {
                if(site.isVariableWriteLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);

                    T.addLockedVariable(variable);
                    T.addLockedVariableType(variable, instruction);

                    System.out.println(transactionID +" wrote to " + variable + " at Site " +
                            sites.get(0).getId() + ": " + value.toString());
                }
            } else if(site.getSiteState().equals("failed")) {
                TM.addToWaitQueue(siteId, T);
            }
        }
    }

    /**
     * Handles all read operations
     * Checks to see whether the read is to all sites or specific site for odd
     * variables. Each have their checks to how to handle the read.
     *
     * <ul>
     *      <ol>First checks to see if the transaction already has a write lock on a variable
     *          and if so reads the value without obtainig a lock</ol>
     *      <ol>Then the sites are checked to see failed and / or locked.  This check is used
     *      to allow the transaction to read if there are any sites not in fail state or locked
     *      variables.</ol>
     *      <ol>If cleared, transaction gets its read locks.  If a site is in a recovered state, it
     *      will not a transaction to read an even if the variable has not be written to.  If it's
     *      odd, the transaction is allowed to read.<</ol>
     *      <ol>If the check doesn't clear, then deadlock is checked</ol>
     *      <ol>If neither of the two, then the site must be in a fail state so add to
     *          site wait queue</ol>
     * </ul>
     *
     * @param T - Transaction Object
     * @param instruction - Instruction Object
     */
    public void read(Transaction T, Instruction instruction) {

        String transactionID = "T" + T.getID().toString();

        Integer value = -1;
        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();


        // Making sure that the site being checked is not down
        if(T.checkLockedVariableType(variable)) {
            value = T.getLockedVariableInfo(variable).getValue();
            System.out.println(transactionID + " read " + variable + ": " + value.toString());
        } else if(index % 2 == 0) {

            if(T.isReadOnly()) {

                Random randomGenerator = new Random();
                Integer siteId;
                Site site;
                site = sites.get(randomGenerator.nextInt(9));

                while(!site.getSiteState().equals("running")) {
                    siteId = randomGenerator.nextInt(9);
                    site = sites.get(siteId - 1);
                }

                value = site.getVariable(variable).getPreviousValue(T.getStartTime());
                System.out.println(transactionID + " read " + variable + ": " + value.toString());

            } else {

                Integer failCheck = MAX_SITES - getFailCount();

                // Counts up the number locked

                // If the number of variables that are not locked is greater than or equal to the
                // failCheck then we can lock
                int not_locked = 0;
                for(Site site: sites){
                    not_locked += (!site.getSiteState().equals("failed") &&
                                   !site.isVariableWriteLocked(variable)) ? 1 : 0;
                }

                if(not_locked >= failCheck) {

                    for(int i = 0; i < sites.size(); i++) {
                        Site site = sites.get(i);
                        if (site.getSiteState().equals("running") || (site.getSiteState().equals("recovered") &&
                                                                      site.getVariable(variable).getOkToRead())) {
                            if (i == 0) {
                                value = site.getVariable(variable).getValue();
                                T.addLockedVariable(variable);
                                T.addLockedVariableType(variable, instruction);
                                System.out.println(transactionID +" read " + variable + ": " + value.toString());
                            }
                            site.lockVariable(T, variable, instruction);
                        } else if(site.getSiteState().equals("recovered") &&
                                  !site.getVariable(variable).getOkToRead()) {
                            TM.addToWaitQueue(site.getId(), T);
                        }
                    }
                } else {
                    TM.addToLockQueue(variable, T);
                }
            }

        } else {

            Integer siteId = 1 + index % 10;
            Site site = sites.get(siteId - 1);

            if(!site.getSiteState().equals("failed")) {

                if(T.isReadOnly()) {
                    value = site.getVariable(variable).getPreviousValue(T.getStartTime());
                    System.out.println(transactionID + " read " + variable + ": " + value.toString());
                } else if(site.isVariableWriteLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);
                    value = site.getVariable(variable).getValue();
                    T.addLockedVariable(variable);
                    T.addLockedVariableType(variable, instruction);
                    System.out.println(transactionID + " read " + variable + ": " + value.toString());
                }
            } else {
                TM.addToWaitQueue(siteId, T);
            }
        }
    }

    /**
     * Handles site failure.  Multiple operations are handled here.  Site is marked
     * as failed and clears the lock table. Also handles the failure of transactions that
     * had locks on variables on the site.
     * @param instruction - fail instruction
     */
    public void fail(Instruction instruction) {
        Integer siteId = instruction.getId();
        sites.get(siteId - 1).setSiteState("failed");

        Set<Transaction> lockedTransactions = sites.get(siteId - 1).getLockedTransactions();

        for(Transaction T : lockedTransactions)
            abort(T);

        sites.get(siteId - 1).clearLocktable();
        failedSiteCount++;
    }

    /**
     * Gets the current site fail count
     * @return Integer
     */
    public Integer getFailCount() {
        return failedSiteCount;
    }

    /**
     * Handles site recovery.  Multiple operations are handled here.  Site is marked
     * as recovered and gets the site to start the recovery process using its
     * defined method.
     * @param instruction - fail instruction
     */
    public void recover(Instruction instruction) {
        Integer siteId = instruction.getId();
        sites.get(siteId - 1).setSiteState("recovered");
        sites.get(siteId - 1).recover();
        failedSiteCount--;
    }


    //private HashMap<String, Integer> varInstructionTracker = new HashMap<String, Integer>();
    //private Set<String> transactionSet = new HashSet<String>();

    public Boolean deadLockCheck(Transaction T, Instruction instruction) {

        String transactionID = "T" + T.getID().toString();
        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();
        String type = instruction.getInstruction();
        Pair<String, String> transType = new Pair<>(type, transactionID);
        //keep track of number of transactions
        this.transactionSet.add(T);

        //private HashMap<String, ArrayList<LinkedList<Pair>>> variableTracker = new HashMap<>();


        //for each key (eg. Wx1, Wx2, Rx2 etc) keep track of the number of accesses
        //String key = instruction.getInstruction() + "x" + instruction.getVariable();
        if (variableTracker.get(variable) != null) {
            // if key already exists, increment the count
            ArrayList<LinkedList<Pair>> temp = variableTracker.get(variable);
            Boolean append = false;
            for(int i = 0; i < temp.size(); i++) {
                for(Pair pair : temp.get(i)) {
                    String pairType = pair.getKey().toString();
                    if((pairType.equals("R") && type.equals("R"))) {
                        append = true;
                        for(Pair check : temp.get(i)) {
                            if(check.getKey().equals("W")) {
                                append = false;
                                break;
                            }
                        }
                    } else if (pair.getValue().toString().equals(transactionID)) {
                        append = true;
                        break;
                    }
                }
                if(append) {
                    temp.get(i).add(transType);
                    variableTracker.put(variable, temp);
                    break;
                }
            }

            if(!append) {
                LinkedList<Pair> tempList = new LinkedList<>();
                tempList.add(transType);
                temp.add(tempList);
                variableTracker.put(variable, temp);
            }

            int count = 0;

            for(HashMap.Entry<String, ArrayList<LinkedList<Pair>>> vars : variableTracker.entrySet()) {
                int interim = 0;
                for(LinkedList<Pair> lists : vars.getValue()) {
                    if(lists.size() > 0)
                        interim++;
                }
                if(interim >= 2) count++;
            }

            if(count == transactionSet.size()) {
                Transaction tAbort = T;
                Integer age = T.getStartTime();
                for(Transaction t : transactionSet) {
                    if(t.getStartTime() > age) {
                        tAbort = t;
                        age = t.getStartTime();
                    }
                }
                System.out.println("T" + tAbort.getID().toString() + " ABORTED due to attempted lock on variable "
                                   + variable);
                abort(tAbort);
                return true;
            }

        } else {
            // if key doesn't exist, put it in the instruction tracker
            LinkedList<Pair> tempList = new LinkedList<>();
            tempList.add(transType);
            ArrayList<LinkedList<Pair>> tempArray = new ArrayList<>();
            tempArray.add(tempList);
            variableTracker.put(variable, tempArray);
        }
        return false;
    }

    /**
     * Handles checking of the lock queue when a transaction
     * releases its locks.  If it's a read, the queue is checked
     * again to allow other read transactions following it to also
     * get a lock
     * @param varId - variable ID
     */
    private void checkLockQueue(String varId) {

        if(!TM.emptyLockQueue(varId)) {
            Transaction T = TM.handleLockQueue(varId);
            Instruction instruction = T.getCurrentInstruction();
            String type = instruction.getInstruction();

            if(type.equals("R")) {
                read(T, instruction);
                String check = "R";
                while(check.equals("R") && !TM.emptyLockQueue(varId)){
                    check = TM.peekLockQueue(varId);
                    if(check.equals("R")) {
                        T = TM.handleLockQueue(varId);
                        read(T, instruction);
                    }
                }
            } else {
                write(T, instruction);
            }
        }
    }

    /**
     * Removes transactions from a lock table if they have been `ed
     * @param T - a transaction object
     */
    public void removeTransLock(Transaction T) {
        for(Site site : sites) {
            site.removeFromLockTable(T);
        }
    }

    private void abort(Transaction T) {

        TM.abort(T);
        String transactionID = "T" + T.getID();
        transactionSet.remove(transactionID);
        HashMap<String, Instruction> variablesLocked = T.getVariablesLockType();

        for(HashMap.Entry<String, Instruction> variable : variablesLocked.entrySet()) {
            Pair<String, String> removePair = new Pair<>(variable.getValue().getInstruction(), transactionID);
            ArrayList<LinkedList<Pair>> lists = variableTracker.get(variable.getKey());
            for(LinkedList<Pair> pairs : lists) {
                pairs.remove(removePair);
            }
        }

        removeTransLock(T);

        Queue<String> variables = T.getVariablesLocked();

        while(!variables.isEmpty()) {
            checkLockQueue(variables.remove());
        }
    }

    /**
     * Terminates a transaction.  The function checks the queue of
     * locked variables in the transaction object and handles the commits
     * and lock releases.
     * @param T - transaction object
     */
    public void end(Transaction T) {

        Queue<String> variables = T.getVariablesLocked();

        while(!variables.isEmpty()) {
            String variable = variables.remove();
            Integer index = Integer.parseInt(variable.replaceAll("\\D+",""));

            if(index % 2 == 0) {
                for(Site site: sites) {
                    if(!site.getSiteState().equals("failed") && (T.getOnSites().get(site.getId()) != 0)
                       && site.isVariableLocked(variable)) {
                        site.handleLockTable(T, variable, TM.getTime());
                    }
                }
            } else {
                Integer site_no = 1 + index % 10;
                Site site = sites.get(site_no - 1);
                site.handleLockTable(T, variable, TM.getTime());
            }
            checkLockQueue(variable);
        }
        //this.safeset.remove()
        T.stopTransaction();
    }

    /**
     * Prints all committed variables at all sites sorted by site
     */
    public void dump() {
        System.out.println("\n=== output of dump ===");
        for(Site site: this.sites){
            int commitCount = 0;
            System.out.println("Site " + site.getId().toString());
            for(HashMap.Entry<String, Variable> variable : site.getAllVariables().entrySet()) {
                if(variable.getValue().checkCommitted()) {
                    System.out.println(variable.getKey() + ": " + variable.getValue().getValue());
                    commitCount++;
                }
            }
            if(commitCount != site.getVariableCount()) {
                System.out.println("All otther variables have their initial values");
            }
            System.out.println("");
        }
    }

    /**
     * Dump function that prints out the committed values of a specific
     * variable at all sites
     * @param x - variable ID
     */
    public void dump(String x) {
        System.out.println("\n=== output of dump ===");
        System.out.println(x);
        for(Site site: this.sites){
            if(site.getVariable(x).checkCommitted()){
                System.out.println("Site " + site.getId().toString() + ": " + site.getVariable(x).getValue());
            }
        }
    }

    /**
     * Dump function that prints out the variables that have been
     * committed at a site
     * @param i - site ID
     */
    public void dump(Integer i) {
        Site site = sites.get(i);
        System.out.println("\n=== output of dump ===");
        int commitCount = 0;
        System.out.println("Site " + site.getId().toString());
        for(HashMap.Entry<String, Variable> variable : site.getAllVariables().entrySet()) {
            if(variable.getValue().checkCommitted()) {
                System.out.println(variable.getKey() + ": " + variable.getValue().getValue());
                commitCount++;
            }
        }
        if(commitCount != site.getVariableCount()) {
            System.out.println("All otther variables have their initial values");
        }
        System.out.println("");
    }
}
