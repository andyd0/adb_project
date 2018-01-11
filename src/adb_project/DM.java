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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.Stack;


public class DM {

    private final int MAX_SITES = 10;
    private int failedSiteCount;
    private ArrayList<Site> sites;
    private Set<Transaction> transactionSet = new HashSet<>();
    private HashMap<Transaction, Boolean> visitedCycleCheck = new HashMap<>();
    private HashMap<Transaction, Boolean> doneCycleCheck = new HashMap<>();


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
     * @param I - Instruction Object
     */
    public void write(Transaction T, Instruction I) {

        Integer index = I.getVariable();
        String varId = "x" + index.toString();


        // Getting a count of failed sites for later checking against what is
        // not locked
        if(index % 2 == 0) {

            if(T.getLockedVariableInfo(varId) != null
                    && T.getLockedVariableInfo(varId).getInstruction().equals("R")) {
                for (Site site : sites) {
                    if (!site.getSiteState().equals("failed") && site.getLockCount(varId) == 1) {
                        site.removeFromLockTable(T);
                        T.removeLockedVariable(varId);
                    }
                }
            }

            int failCheck = MAX_SITES - getFailCount();

            // Counts up the number locked

            // If the number of variables that are not locked is greater than or equal to the
            // failCheck then we can lock
            int not_locked = 0;
            for(Site site: sites){
                not_locked += (!site.getSiteState().equals("failed") && !site.isVariableLocked(varId)) ? 1 : 0;
            }

            if(not_locked >= failCheck) {
                for (Site site : sites) {
                    if (!site.getSiteState().equals("failed")) {
                        site.lockVariable(T, varId, I);
                    }
                }

                T.addLockedVariable(varId);
                T.addLockedVariableType(varId, I);
                T.removeFromDependsOn(varId);

            } else {
                checkDependenceOn(varId, T);
                TM.addToLockQueue(varId, T);
            }

        } else {

            Integer siteId = 1 + index % 10;
            Site site = sites.get(siteId - 1);

            if(T.getLockedVariableInfo(varId) != null
                    && T.getLockedVariableInfo(varId).getInstruction().equals("R")
                    && site.getLockCount(varId) == 1) {
                if (!site.getSiteState().equals("failed")) {
                    site.removeFromLockTable(T);
                    T.removeLockedVariable(varId);
                }
            }

            if(!site.getSiteState().equals("failed")) {
                if(site.isVariableLocked(varId)) {
                    checkDependenceOn(varId, T);
                    TM.addToLockQueue(varId, T);
                } else {
                    site.lockVariable(T, varId, I);

                    T.addLockedVariable(varId);
                    T.addLockedVariableType(varId, I);
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
     * @param I - Instruction Object
     */
    public void read(Transaction T, Instruction I) {

        String transactionID = "T" + T.getID().toString();

        Integer value = -1;
        Integer index = I.getVariable();
        String varId = "x" + index.toString();


        // Making sure that the site being checked is not down
        if(index % 2 == 0) {

            if(T.isReadOnly()) {

                Random randomGenerator = new Random();
                Integer siteId;
                Site site;
                site = sites.get(randomGenerator.nextInt(9));

                while(!site.getSiteState().equals("running")) {
                    siteId = randomGenerator.nextInt(9);
                    site = sites.get(siteId);
                }

                value = site.getVariable(varId).getPreviousValue(T.getStartTime());
                System.out.println(transactionID + " read " + varId+ ": " + value.toString());

            } else {

                int failCheck = MAX_SITES - getFailCount();

                // Counts up the number locked

                // If the number of variables that are not locked is greater than or equal to the
                // failCheck then we can lock
                int not_locked = 0;
                for(Site site: sites){
                    not_locked += (!site.getSiteState().equals("failed") &&
                            !site.isVariableWriteLocked(varId)) ? 1 : 0;
                }

                if(not_locked >= failCheck) {

                    for(int i = 0; i < sites.size(); i++) {
                        Site site = sites.get(i);
                        if (site.getSiteState().equals("running") || (site.getSiteState().equals("recovered") &&
                                site.getVariable(varId).getOkToRead())) {
                            if (i == 0) {
                                value = site.getVariable(varId).getValue();
                                T.addLockedVariable(varId);
                                T.addLockedVariableType(varId, I);
                                System.out.println(transactionID +" read " + varId + ": " + value.toString());
                            }
                            site.lockVariable(T, varId, I);
                        } else if(site.getSiteState().equals("recovered") &&
                                !site.getVariable(varId).getOkToRead()) {
                            TM.addToWaitQueue(site.getId(), T);
                        }
                    }
                } else {
                    checkDependenceOn(varId, T);
                    TM.addToLockQueue(varId, T);
                }
            }

        } else {

            Integer siteId = 1 + index % 10;
            Site site = sites.get(siteId - 1);

            if(!site.getSiteState().equals("failed")) {

                if(T.isReadOnly()) {
                    value = site.getVariable(varId).getPreviousValue(T.getStartTime());
                    System.out.println(transactionID + " read " + varId + ": " + value.toString());
                } else if(site.isVariableWriteLocked(varId)) {
                    checkDependenceOn(varId, T);
                    TM.addToLockQueue(varId, T);
                } else {
                    site.lockVariable(T, varId, I);
                    value = site.getVariable(varId).getValue();
                    T.addLockedVariable(varId);
                    T.addLockedVariableType(varId, I);
                    System.out.println(transactionID + " read " + varId + ": " + value.toString());
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
     * @param I - fail instruction
     */
    public void fail(Instruction I) {
        Integer siteId = I.getId();
        sites.get(siteId - 1).setSiteState("failed");

        Set<Transaction> lockedTransactions = sites.get(siteId - 1).getLockedTransactions();

        for(Transaction T : lockedTransactions)
            abort(T);

        sites.get(siteId - 1).clearLocktable();
        failedSiteCount++;
    }


    /**
     * Gets the current site fail count
     * @return int - fail site count
     */
    private int getFailCount() {
        return failedSiteCount;
    }


    /**
     * Handles site recovery.  Multiple operations are handled here.  Site is marked
     * as recovered and gets the site to start the recovery process using its
     * defined method.
     * @param I - recover instruction
     */
    public void recover(Instruction I) {

        Integer siteId = I.getId();
        sites.get(siteId - 1).setSiteState("recovered");
        sites.get(siteId - 1).recover();
        failedSiteCount--;

        while(TM.checkWaitQueue(siteId)) {
            Queue<Transaction> transactions = TM.getWaitQueue(siteId);
            while(!transactions.isEmpty()) {
                Transaction T = transactions.remove();
                Instruction tTnstruction = T.getCurrentInstruction();
                if(tTnstruction.getInstruction().equals("R")) {
                    read(T, tTnstruction);
                } else {
                    write(T, tTnstruction);
                }
            }
        }
    }


    public void deadLockCheck(Transaction T) {

        transactionSet.add(T);
        String currentVarId = "x" + T.getCurrentInstruction().getVariable().toString();

        if(transactionSet.size() > 1) {

            HashMap<Transaction, LinkedList<Transaction>> adjacencyList = new HashMap<>();
            HashMap<String, LinkedList<Transaction>> dependsOn = T.getDependsOn();
            LinkedList<Transaction> dependsOnList = new LinkedList<>();
            Set<Transaction> dependsOnSet = new HashSet<>();

            for(HashMap.Entry<String, LinkedList<Transaction>> var : dependsOn.entrySet()) {
                LinkedList<Transaction> temp = var.getValue();
                if(!temp.isEmpty())
                    dependsOnSet.addAll(temp);
            }

            dependsOnList.addAll(dependsOnSet);
            adjacencyList.put(T, dependsOnList);

            Set<Transaction> checkTransactions = new HashSet<>();

            Stack<Transaction> tempCheck = new Stack<>();
            tempCheck.addAll(dependsOnSet);

            // Now getting all other adjacency lists...
            while(!tempCheck.isEmpty()) {
                Transaction t = tempCheck.pop();
                LinkedList<Transaction> adjacencyCheck = buildDependsOn(t);
                if(!t.equals(T)) {
                    adjacencyList.put(t, buildDependsOn(t));
                    checkTransactions.add(t);
                    for(Transaction transaction : adjacencyCheck){
                        if(!tempCheck.contains(transaction))
                            tempCheck.add(transaction);
                    }
                }
            }

            checkTransactions.add(T);

            // Checks for cycle
            int numNodesToCheck = checkTransactions.size();

            if(numNodesToCheck > 1) {

                Boolean cycleExists;

                // Will be used in Cycle checker
                for(Transaction t : checkTransactions) {
                    visitedCycleCheck.put(t, false);
                    doneCycleCheck.put(t, false);
                }

                // Checking for cycle
                cycleExists = checkGraph(adjacencyList);

                // Purge if there is a cycle
                if(cycleExists) {
                    Transaction tAbort = T;
                    Integer age = T.getStartTime();
                    for(Transaction t : checkTransactions) {
                        if(t.getStartTime() > age) {
                            tAbort = t;
                            age = t.getStartTime();
                        }
                    }
                    System.out.println("T" + tAbort.getID().toString() + " ABORTED due to attempted lock on variable "
                            + currentVarId);
                    abort(tAbort);
                }
            }
        }
    }


    private LinkedList<Transaction> buildDependsOn(Transaction T) {
        LinkedList<Transaction> dependsOn = new LinkedList<>();
        if(T.checkDependsOn()) {
            HashMap<String, LinkedList<Transaction>> dOn = new HashMap<>(T.getDependsOn());
            for(HashMap.Entry<String, LinkedList<Transaction>> ds : dOn.entrySet()) {
                LinkedList<Transaction> ts = ds.getValue();
                dependsOn.addAll(ts);
            }
        }
        return dependsOn;
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


    private void checkDependenceOn(String varId, Transaction T) {
        Site site = getSite(Integer.parseInt(varId.replaceAll("\\D+","")));
        Set<Transaction> tLocks = site.getTransactionsLockedOnVariable(varId);
        Set<Transaction> tQueue = TM.getTransactionsFromLockQueue(varId);

        if(tLocks != null) {
            for (Transaction t : tLocks) {
                if(!t.equals(T)) {
                    T.addToDependsOn(varId, t);
                }
            }
        }

        if(tQueue != null)
            for(Transaction t : tQueue)
                t.addToDependsOn(varId, T);
    }


    /**
     * Removes transactions from a lock table if they have been `ed
     * @param T - a transaction object
     */
    private void removeTransLock(Transaction T) {
        for(Site site : sites) {
            site.removeFromLockTable(T);
        }
    }


    /**
     * Gets a random live site or odd site
     * @param index - Site number
     */
    private Site getSite(Integer index) {
        Site site;
        if (index % 2 == 0) {
            Random randomGenerator = new Random();
            Integer siteId;
            site = sites.get(randomGenerator.nextInt(9));

            while(!site.getSiteState().equals("running")) {
                siteId = randomGenerator.nextInt(9);
                site = sites.get(siteId);
            }
        } else {
            Integer siteId = 1 + index % 10;
            site = sites.get(siteId - 1);
        }
        return site;
    }


    /**
     * Handles trasnaction abort from the DM side and passes abort duties to TM as well
     * Removes any locks a transaction has and removed from any lock queue
     * @param T - a transaction object
     */
    private void abort(Transaction T) {

        TM.abortTransaction(T);

        removeTransLock(T);
        transactionSet.remove(T);

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

            String varId = variables.remove();
            Integer index = Integer.parseInt(varId.replaceAll("\\D+",""));

            Integer value = null;

            if(index % 2 == 0) {
                for(Site site: sites) {
                    if(!site.getSiteState().equals("failed") && (T.getOnSites().get(site.getId()) != 0)
                            && site.isVariableLocked(varId)) {
                        value = site.handleLockTable(T, varId, TM.getTime());
                    }
                }
                if(value != null)
                    System.out.println("T" + T.getID().toString() + " committed " + varId +
                            " to all available sites " + ": " + value.toString());

            } else {
                Integer site_no = 1 + index % 10;
                Site site = sites.get(site_no - 1);
                value = site.handleLockTable(T, varId, TM.getTime());
                if(value != null)
                    System.out.println("T" + T.getID().toString() +" committed " + varId + " to Site " +
                            sites.get(0).getId() + ": " + value.toString());
            }
            checkLockQueue(varId);
        }
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
                System.out.println("All other variables have their initial values");
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
            System.out.println("All other variables have their initial values");
        }
        System.out.println("");
    }


    /**
     * Inspired by Siegel's DFS cycle check pseudocode of a directed graph.
     * This is the driver.
     * @param adjacencyList - HashMap of transactions and their adjanency lists
     */
    private Boolean checkGraph(HashMap<Transaction, LinkedList<Transaction>> adjacencyList) {

        Boolean foundCycle = false;
        ArrayList<Boolean> cycleTracking = new ArrayList<>();

        for(HashMap.Entry<Transaction, Boolean> toVisit : visitedCycleCheck.entrySet()) {
            if(!toVisit.getValue()) {
                dfsCycleCheck(adjacencyList, toVisit.getKey(), cycleTracking);
            }
        }

        // Since the DFS is done recursively, an array list cycleTracking is filled
        // with Booleans when checking for cycles using DFS and then checked here
        // to see if a cycle was found
        for(Boolean check : cycleTracking) {
            if(check)
                foundCycle = true;
        }

        return foundCycle;
    }


    /**
     * Actual DFS into directed graph via adjacency lists
     * @param adjacencyList - HashMap of transactions and their adjanency lists
     * @param v - parent Transaction
     * @param cycleTracking - keeps track of whether a cycle has been found
     */
    private void dfsCycleCheck(HashMap<Transaction, LinkedList<Transaction>> adjacencyList, Transaction v,
                               ArrayList<Boolean> cycleTracking) {

        // Tagging parent Transaction as seen
        visitedCycleCheck.put(v, true);

        Stack<Transaction> neighbors = new Stack<>();

        // Getting parent Transaction's neighbors
        if(adjacencyList.get(v) != null)
            neighbors.addAll(adjacencyList.get(v));

        // DFS neighors.  If visited but not done yet,
        // there is a cycle
        while(!neighbors.isEmpty()) {
            Transaction w = neighbors.pop();
            if (!visitedCycleCheck.get(w)) {
                cycleTracking.add(false);
                dfsCycleCheck(adjacencyList, w, cycleTracking);
            } else if (!doneCycleCheck.get(w)) {
                cycleTracking.add(true);
            }
        }

        // Tagging parent Transaction as done
        doneCycleCheck.put(v, true);
    }


    /**
     * Actual DFS into directed graph via adjacency lists
     * @param T - transaction object
     * @param I - current instruction for transaction
     * @return Boolean - returns t/f if trnsaction has a write lock on variable
     */
    public Boolean hasWriteLock(Transaction T, Instruction I) {
        String variable = "x" + I.getVariable().toString();
        String transactionID = "T" + T.getID();
        Integer value;
        if (T.checkLockedVariableType(variable) != null && T.checkLockedVariableType(variable).equals("W")) {
            value = T.getLockedVariableInfo(variable).getValue();
            System.out.println(transactionID + " read " + variable + ": " + value.toString());
            return true;
        }
        return false;
    }
}
