package adb_project;

import java.util.ArrayList;
import java.util.Queue;
import java.util.Random;

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
                not_locked += (!site.isVariableLocked(variable) && site.getSiteState().equals("running")) ? 1 : 0;
            }

            if(not_locked >= failCheck) {
                for(Site site: sites){
                    if(site.isRunning()) {
                        site.lockVariable(T, variable, instruction);
                    }
                }

                T.addLockedVariable(index);

                System.out.println(transactionID +" wrote to " + variable + " to all sites " + ": " + value.toString());

              // This needs to be implemented
            } else if(deadLockCheck()) {
                abort(T);
                // Since the variable is locked or cannot be accessed,
                // add to lock queue
            } else {
                TM.addToLockQueue(variable, T);
            }

        } else {

            Integer siteID = 1 + index % 10;
            Site site = sites.get(siteID - 1);

            if(!site.getSiteState().equals("failed")) {
                if(site.isVariableLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);

                    T.addLockedVariable(index);

                    System.out.println(transactionID +" wrote to " + variable + " at Site " +
                            sites.get(0).getSiteNum() + ": " + value.toString());
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

                while(!site.isRunning()) {
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
                    not_locked += (!site.isVariableLocked(variable) && site.isRunning()) ? 1 : 0;
                }

                if(not_locked >= failCheck) {

                    for(int i = 0; i < sites.size(); i++) {
                        Site site = sites.get(i);
                        if(site.getSiteState().equals("running") && i == 0) {
                            value = site.getVariable(variable).getData();
                            site.lockVariable(T, variable, instruction);
                        } else if(site.getSiteState().equals("running")) {
                            site.lockVariable(T, variable, instruction);
                        }
                    }

                    T.addLockedVariable(index);
                    System.out.println(transactionID +" read " + variable + ": " + value.toString());

                // This needs to be implemented
                } else if(deadLockCheck()) {
                    abort(T);
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
                } else if(site.isVariableLocked(variable)) {
                    TM.addToLockQueue(variable, T);
                } else {
                    site.lockVariable(T, variable, instruction);
                    value = site.getVariable(variable).getData();
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
    // TODO: Clear locktable for odd variables?  Unclear atm on how to handle
    public void fail(Instruction instruction) {
        Integer siteID = instruction.getID();
        sites.get(siteID).setSiteState("failed");
        sites.get(siteID).clearLocktable();
        failedSites[siteID - 1] = 1;
        failedSiteCount++;
    }

    public Integer getFailCount() {
        return failedSiteCount;
    }

    // Handles recovering a site
    public void recover(Instruction instruction) {
        Integer siteID = instruction.getID();
        sites.get(siteID).setSiteState("recovered");
        failedSites[siteID - 1] = 0;
        failedSiteCount--;
    }

    public void abort(Transaction T) {

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


    // Ends a transaction
    // TODO: This needs to also update the value
    public void end(Transaction T) {

        Queue<Integer> variables = T.getVariablesLocked();

        while(!variables.isEmpty()) {
            Integer index = variables.remove();
            String variable = "x" + index.toString();

            if(index % 2 == 0) {
                for(Site site: sites) {
                    if(site.isRunning()) {
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
//        if(instruction_split.length > 1){
//            System.out.println("For now");
//        } else {
//            for(Site site: sites) {
//                System.out.println(site.toString());
//            }
//        }
    }
}
