package adb_project;

import java.util.*;

public class DM {

    private final Integer MAX_SITES = 10;
    private int failedSiteCount;

    private ArrayList<Site> sites;
    private int[] failedSites;

    // FailedSites = bit vector to keep track of failed sites
    // A count as well to keep track of count without having to go through
    // the bit array

    public DM() {
        failedSiteCount = 0;
        failedSites = new int[MAX_SITES];
        sites = initializeSites();
    }

    // Just a wrapper so that the instruction can be sent to the function that
    // will check for locks etc.
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
                for(Site site: sites){
                    if(!site.getSiteState().equals("failed")) {
                        site.lockVariable(T, variable, instruction);
                    }
                }

                T.addLockedVariable(index);

                System.out.println(transactionID +" wrote to " + variable + " to all sites " + ": " + value.toString());

              // This needs to be implemented
            } else if(deadLockCheck()) {
                TM.abort(T);
                // Since the variable is locked or cannot be accessed,
                // add to lock queue
            } else {
                TM.addToLockQueue(variable, T);
            }

        } else {

            Integer siteID = 1 + index % 10;
            Site site = sites.get(siteID - 1);

            if(!site.getSiteState().equals("failed")) {
                if(site.isVariableWriteLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);

                    T.addLockedVariable(index);

                    System.out.println(transactionID +" wrote to " + variable + " at Site " +
                            sites.get(0).getId() + ": " + value.toString());
                }
            } else if(site.getSiteState().equals("failed")) {
                TM.addToWaitQueue(siteID, T);
            }
        }
    }

    // Handles read only or passes read locks to the function that will
    // check for locks etc
    public void read(Transaction T, Instruction instruction) {

        String transactionID = "T" + T.getID().toString();

        Integer value = -1;
        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();

        // Making sure that the site being checked is not down
        if(index % 2 == 0) {

            if(T.isReadOnly()) {

                Random randomGenerator = new Random();
                Integer siteID;
                Site site;
                site = sites.get(randomGenerator.nextInt(9));

                while(!site.getSiteState().equals("running")) {
                    siteID = randomGenerator.nextInt(9);
                    site = sites.get(siteID - 1);
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
                                T.addLockedVariable(index);
                                System.out.println(transactionID +" read " + variable + ": " + value.toString());
                            }
                            site.lockVariable(T, variable, instruction);
                        } else if(site.getSiteState().equals("recovered") &&
                                  !site.getVariable(variable).getOkToRead()) {
                            TM.addToWaitQueue(site.getId(), T);
                        }
                    }
                // This needs to be implemented
                } else if(deadLockCheck()) {
                    TM.abort(T);
                    // Since the variable is locked or cannot be accessed,
                    // add to lock queue

                } else {
                    TM.addToLockQueue(variable, T);
                }
            }

        } else {

            Integer siteID = 1 + index % 10;
            Site site = sites.get(siteID - 1);

            if(!site.getSiteState().equals("failed")) {

                if(T.isReadOnly()) {
                    value = site.getVariable(variable).getPreviousValue(T.getStartTime());
                    System.out.println(transactionID + " read " + variable + ": " + value.toString());
                } else if(site.isVariableWriteLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);
                    value = site.getVariable(variable).getValue();
                    T.addLockedVariable(index);
                    System.out.println(transactionID + " read " + variable + ": " + value.toString());
                }
            } else {
                TM.addToWaitQueue(siteID, T);
            }
        }
    }


     // Creates the sites for the DM
    private ArrayList<Site> initializeSites() {
        ArrayList<Site> sites = new ArrayList<>();
        for (int i = 1; i <= MAX_SITES; i++) {
            Site s = new Site(i);
            sites.add(s);
        }
        return sites;
    }

    // Handles failing a site
    public void fail(Instruction instruction) {
        Integer siteID = instruction.getId();
        sites.get(siteID - 1).setSiteState("failed");

        Set<Transaction> lockedTransactions = sites.get(siteID - 1).getLockedTransactions();
        removeTransLock(lockedTransactions);

        sites.get(siteID - 1).clearLocktable();
        failedSites[siteID - 1] = 1;
        failedSiteCount++;
    }

    public Integer getFailCount() {
        return failedSiteCount;
    }

    // Handles recovering a site
    public void recover(Instruction instruction) {
        Integer siteID = instruction.getId();
        sites.get(siteID - 1).setSiteState("recovered");
        sites.get(siteID - 1).recover();
        failedSites[siteID - 1] = 0;
        failedSiteCount--;
    }

    public Boolean deadLockCheck() {
        return false;
    }

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


    public void removeTransLock(Set<Transaction> lockedTransactions) {
        for(Site site : sites) {
            for(Transaction T : lockedTransactions)
            site.removeFromLockTable(T);
        }
    }

     // Ends a transaction
    public void end(Transaction T) {

        Queue<Integer> variables = T.getVariablesLocked();

        while(!variables.isEmpty()) {
            Integer index = variables.remove();
            String variable = "x" + index.toString();

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
        T.stopTransaction();
    }

    public void dump(Instruction instruction) {
        ArrayList<Site> sites = this.sites;
        System.out.println("=== output of dump ===");
        for(Site site: sites){
            int i = 0;
            System.out.println("Site " + site.getId().toString());
            for(HashMap.Entry<String, Variable> variable : site.getAllVariables().entrySet()) {
                if(variable.getValue().checkCommitted()) {
                    System.out.println(variable.getKey() + ": " + variable.getValue().getValue());
                    i++;
                }
            }
            if(i != site.getVariableCount()) {
                System.out.println("All otther variables have their initial values");
            }
            System.out.println("");
        }
    }

    public void dump() {
        System.out.println("\n=== output of dump ===");
        for(Site site: this.sites){
            int i = 0;
            System.out.println("Site " + site.getId().toString());
            for(HashMap.Entry<String, Variable> variable : site.getAllVariables().entrySet()) {
                if(variable.getValue().checkCommitted()) {
                    System.out.println(variable.getKey() + ": " + variable.getValue().getValue());
                    i++;
                }
            }
            if(i != site.getVariableCount()) {
                System.out.println("All otther variables have their initial values");
            }
            System.out.println("");
        }
    }

    public void dump(String x) {
        System.out.println("\n=== output of dump ===");
        System.out.println(x);
        for(Site site: this.sites){
            if(site.getVariable(x).checkCommitted()){
                System.out.println("Site " + site.getId().toString() + ": " + site.getVariable(x).getValue());
            }
        }
    }

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
