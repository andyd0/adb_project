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

    private final Integer MAX_SITES = 10;
    private int failedSiteCount;
    private ArrayList<Site> sites;
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

        Integer index = instruction.getVariable();
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

            Integer failCheck = MAX_SITES - getFailCount();

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
                        site.lockVariable(T, varId, instruction);
                    }
                }

                T.addLockedVariable(varId);
                T.addLockedVariableType(varId, instruction);
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
                    site.lockVariable(T, varId, instruction);

                    T.addLockedVariable(varId);
                    T.addLockedVariableType(varId, instruction);
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
        if(T.checkLockedVariableType(variable) != null && T.checkLockedVariableType(variable).equals("W")) {
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
                    site = sites.get(siteId);
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
                    checkDependenceOn(variable, T);
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
                    checkDependenceOn(variable, T);
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


    public Boolean deadLockCheck(Transaction T, Instruction instruction) {

        transactionSet.add(T);
        int[][] adjacency;

        if(transactionSet.size() > 1) {
            Integer index = instruction.getVariable();
            String currentVarId = "x" + index;

            ArrayList<ArrayList<Integer>> matrix = new ArrayList<>();

            int max = 0;
            for (Transaction t : transactionSet) {
                max = (t.getID() > max) ? t.getID() : max;
            }

            for (int i = 0; i < matrix.size(); i++) {
                Integer tmpSize = matrix.get(i).size();
                for (int j = tmpSize; j < max; j++) {
                    matrix.get(i).add(0);
                }
            }
            for (int i = matrix.size(); i < max; i++) {
                matrix.add(i, new ArrayList<>(max));
                for (int j = 0; j < max; j++) {
                    matrix.get(i).add(0);
                }
            }

            Site site = getSite(index);
            Boolean varLocked = false;

            if((T.checkLockedVariableType(currentVarId) != null
                    && T.checkLockedVariableType(currentVarId).equals("R"))) {
                varLocked = false;
            } else if(site.isVariableLocked(currentVarId)) {
                varLocked = true;
            }

            Set<Transaction> allTransactions = new HashSet<>();
            HashMap<Integer, LinkedList<Transaction>> dependsOnAll = new HashMap<>();
            HashMap<Integer, LinkedList<Transaction>> dependsOnItAll = new HashMap<>();

            LinkedList<Transaction> currentBuildDependsOn = buildDependsOn(T);
            LinkedList<Transaction> currentBuildDependsOnIt = buildDependsOnIt(T);

            if (varLocked) {
                Set<Transaction> ts = site.getTransactionsLockedOnVariable(currentVarId);
                for (Transaction t : ts) {
                    if (currentBuildDependsOn.isEmpty() || !currentBuildDependsOn.contains(t))
                        currentBuildDependsOn.add(t);
                }
            }

            dependsOnAll.put(T.getID(), currentBuildDependsOn);
            dependsOnItAll.put(T.getID(), currentBuildDependsOnIt);


            for(Transaction t : transactionSet) {
                LinkedList<Transaction> dependsCheck = buildDependsOn(t);
                LinkedList<Transaction> dependsOnItCheck = buildDependsOnIt(t);
                if(dependsCheck.contains(T) || dependsOnItCheck.contains(T)) {
                    dependsOnAll.put(t.getID(), buildDependsOn(t));
                    LinkedList<Transaction> dependsOnItTemp = buildDependsOnIt(t);
                    if(t.checkLockedVariableType(currentVarId) != null && varLocked) {
                        dependsOnItTemp.add(T);
                    }
                    dependsOnItAll.put(t.getID(), dependsOnItTemp);
                }
            }

            for(HashMap.Entry<Integer, LinkedList<Transaction>> tSetDepsOn : dependsOnAll.entrySet()) {
                LinkedList<Transaction> ts = tSetDepsOn.getValue();
                while (!ts.isEmpty()) {
                    Transaction t = ts.remove();
                    allTransactions.add(t);
                    matrix.get(t.getID() - 1).set(T.getID() - 1, 1);
                }
            }

            for(HashMap.Entry<Integer, LinkedList<Transaction>> tSetDepsOnIt : dependsOnItAll.entrySet()) {
                LinkedList<Transaction> ts = tSetDepsOnIt.getValue();
                while (!ts.isEmpty()) {
                    Transaction t = ts.remove();
                    allTransactions.add(t);
                    matrix.get(T.getID() - 1).set(t.getID() - 1, 1);
                }
            }

            adjacency = new int[max][max];
            for (int i = 0; i < max; i++) {
                for (int j = 0; j < max; j++) {
                    adjacency[i][j] = matrix.get(i).get(j);
                }
            }

//            for (int i = 0; i < max; i++) {
//                for (int j = 0; j < max; j++) {
//                    System.out.print(adjacency[i][j]);
//                }
//                System.out.println();
//            }
//            System.out.println();

            Boolean cycleExists = false;

            if (adjacency[0].length > 1) {
                cycleExists = checkGraph(adjacency, 1);
            }

            allTransactions.add(T);

            if(cycleExists) {
                Transaction tAbort = T;
                Integer age = T.getStartTime();
                for(Transaction t : allTransactions) {
                    if(t.getStartTime() > age) {
                        tAbort = t;
                        age = t.getStartTime();
                    }
                }
                System.out.println("T" + tAbort.getID().toString() + " ABORTED due to attempted lock on variable "
                        + currentVarId);
                abort(tAbort);
                return true;
            }

            return cycleExists;
        }
        return false;
    }

    private LinkedList<Transaction> buildDependsOnIt(Transaction T) {
        LinkedList<Transaction> dependsOnIt = new LinkedList<>();
        if(T.checkDependsOnIt()) {
            HashMap<String, LinkedList<Transaction>> dOnIt = new HashMap<>(T.getDependsOnIt());
            for(HashMap.Entry<String, LinkedList<Transaction>> ds : dOnIt.entrySet()) {
                LinkedList<Transaction> ts = ds.getValue();
                    dependsOnIt.addAll(ts);
            }
        }
        return dependsOnIt;
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

        if(tLocks != null)
            for(Transaction t : tLocks) {
                t.addToDependsOnIt(varId, T);
                T.addToDependsOn(varId, t);
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


    private void abort(Transaction T) {

        TM.abort(T);

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

            String varId = variables.remove();
            T.removeFromDependsOnIt(varId);
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


    private Boolean checkGraph(int adjacency_matrix2[][], int source) {

        Stack<Integer> stack = new Stack<>();
        int adjacencyMatrix[][];
        int adjacency_matrix[][] = new int[adjacency_matrix2[0].length + 1][adjacency_matrix2[0].length+1];

        for (int i = 0; i < adjacency_matrix2[0].length + 1; i++) {
            for (int j = 0; j < adjacency_matrix2[0].length + 1; j++) {
                if (i == 0) {
                    adjacency_matrix[i][j] = 0;
                } else if (j == 0) {
                    adjacency_matrix[i][j] = 0;
                } else {
                    adjacency_matrix[i][j] = adjacency_matrix2[i - 1][j - 1];
                }
            }
        }

        int number_of_nodes = adjacency_matrix[source].length - 1;

        adjacencyMatrix = new int[number_of_nodes + 1][number_of_nodes + 1];
        for (int sourcevertex = 1; sourcevertex <= number_of_nodes; sourcevertex++) {
            for (int destinationvertex = 1; destinationvertex <= number_of_nodes; destinationvertex++) {
                adjacencyMatrix[sourcevertex][destinationvertex] =
                        adjacency_matrix[sourcevertex][destinationvertex];
            }
        }

        int visited[] = new int[number_of_nodes + 1];
        int element = source;
        int destination = source;
        visited[source] = 1;
        stack.push(source);

        while (!stack.isEmpty()) {
            element = stack.peek();
            destination = element;
            while (destination <= number_of_nodes) {
                if (adjacencyMatrix[element][destination] == 1 && visited[destination] == 1) {
                    if (stack.contains(destination)) {
                        return true;
                    }
                }

                if (adjacencyMatrix[element][destination] == 1 && visited[destination] == 0) {
                    stack.push(destination);
                    visited[destination] = 1;
                    adjacencyMatrix[element][destination] = 0;
                    element = destination;
                    destination = 1;
                    continue;
                }
                destination++;
            }
            stack.pop();
        }
        return false;
    }
}
