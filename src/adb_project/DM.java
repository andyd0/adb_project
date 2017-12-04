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
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

public class DM {

    private final Integer MAX_SITES = 10;
    private int failedSiteCount;
    private ArrayList<Site> sites;
    private Set<String> safeTransactionSet = new HashSet<String>();

    private HashMap<String, Integer> varInstructionTracker = new HashMap<String, Integer>();
    private Set<String> transactionSet = new HashSet<String>();

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
        deadLockCheck(T, instruction);

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
                for(Site site: sites){
                    if(!site.getSiteState().equals("failed")) {
                        site.lockVariable(T, variable, instruction);
                    }
                }

                T.addLockedVariable(variable);
                T.addLockedVariableType(variable, instruction);

                System.out.println(transactionID +" wrote to " + variable + " to all sites " + ": " + value.toString());

              // This needs to be implemented
            } /*else if(deadLockCheck(T, instruction)) {
                System.out.println("Deadlock exists");
                // TM.abort(T);
                // Since the variable is locked or cannot be accessed,
                // add to lock queue

                // TM.abort(T.getID());
                // this.safeTransactionSet.remove(T.getID());
            } */ else {
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
        deadLockCheck(T, instruction);

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
                // This needs to be implemented
                } /*else if(deadLockCheck(T, instruction)) {
                    System.out.println("Deadlock exists");
                    // TM.abort(T);
                    // Since the variable is locked or cannot be accessed,
                    // add to lock queue

                    // TM.abort(T.getID());
                    // this.safeTransactionSet.remove(T.getID());
                } */else {
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
        removeTransLock(lockedTransactions);

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

        //keep track of number of transactions
        this.transactionSet.add(transactionID);

        //for each key (eg. Wx1, Wx2, Rx2 etc) keep track of the number of accesses
        String key = instruction.getInstruction() + "x" + instruction.getVariable();
        if (varInstructionTracker.get(key) != null) {
            // if key already exists, increment the count
            Integer val = varInstructionTracker.get(key);
            val++;
            varInstructionTracker.put(key, val);

            if (key.substring(0,1).equals("W")) {
                if (val >= transactionSet.size()) {
                    System.out.println("--------- DEADLOCK FOUND ---------");
                }
            }
        } else {
            // if key doesn't exist, put it in the instruction tracker
            varInstructionTracker.put(key, 0);
        }
        return false;
    }

    /**
     * Handles Deadlock check.  Needs to be implemented
     * @return Boolean
     */
    /*
    public Boolean deadLockCheck(Transaction T, Instruction instruction) {
        String transactionID = "T" + T.getID().toString();
        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();
        Integer siteId;
        Site site;

        // 1. get the site on which the current variable exists
        if(index % 2 == 0) {
            Random randomGenerator = new Random();
            site = sites.get(randomGenerator.nextInt(9));

            while(!site.getSiteState().equals("running")) {
                siteId = randomGenerator.nextInt(9);
                site = sites.get(siteId - 1);
            }
        } else {
            siteId = 1 + index % 10;
            site = sites.get(siteId - 1);
        }

        // 2. using site info: get transaction (Tw) that holds a lock on the current variable
        Transaction Tw = site.getTransactionThatLockedVariable(variable);

        // no transaction already locking the variable, this is safe so continue
        if (Tw == null) {
            return false;
        }

        // 3. if Tw exists then the current transaction (T from the function args)
        // will need to wait on Tw

        // Check if Tw already exists in the set, if it does, this will lead to a cycle
        String existingTransactionId = "T" + Tw.getID().toString();
        Integer existingTID = Tw.getID();

        System.out.println("-------- existingTransactionId: " + existingTransactionId);
        System.out.println("-------- transactionID: " + transactionID);

        if (existingTransactionId.equals(transactionID)) {
            System.out.println("-------- Same transaction found!");
            return false;
        }

        if (this.safeTransactionSet.contains(existingTransactionId)) {
            // deadlock exists so we remove this transaction from the safe set
            // and abort it
            System.out.println("---------- DEADLOCK FOUND ----------");
            this.safeTransactionSet.remove(T.getID());
            return true;
        } else {
            //System.out.print("Adding current Transaction: T" + T.getID().toString());
            //System.out.print(" and existing Transaction Tw: T" + Tw.getID().toString());
            //System.out.print(" to the safeset\n");
            this.safeTransactionSet.add("T" + T.getID().toString());
            this.safeTransactionSet.add("T" + Tw.getID().toString());
        }

        System.out.println("----------");
        System.out.println("safeTransactionSet currently has:");

        // Print Transaction IDs already existing in the set
        Iterator<String> iterator = safeTransactionSet.iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            System.out.println(name);
        }

        System.out.println("----------");
        return false;
    }
    */

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
     * Removes transactions from a lock table if they have been aborted
     * @param lockedTransactions - set of transaction objects
     */
    public void removeTransLock(Set<Transaction> lockedTransactions) {
        for(Site site : sites) {
            for(Transaction T : lockedTransactions)
            site.removeFromLockTable(T);
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
