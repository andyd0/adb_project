package adb_project;

import java.util.ArrayList;
import java.util.Queue;
import java.util.Random;

public class DM {

    private final Integer MAX_SITES = 10;
    private int failedSiteCount;

    private ArrayList<Site> sites;
    private int[] failedSites;

    // Added
    // Variable History for multi read - should this be at the DM?
    // Single point of failure

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
        handleOperation(T, instruction,"W");
    }

    // Handles read only or passes read locks to the function that will
    // check for locks etc
    public void read(Transaction T, Instruction instruction) {

        String transactionID = "T" + T.getID().toString();

        Integer siteID;
        Integer value = -1;
        Integer index = instruction.getVariable();
        String variable = "x" + index.toString();

        // Making sure that the site being checked is not down
        Site site = getRandomSite(index);

        if(T.isReadOnly()) {
            value = site.getVariable(variable).getPreviousValue();
            System.out.println(transactionID +" read " + variable + ": " + value.toString());
        } else {
            handleOperation(T, instruction,"R");
        }
    }

    // Checks whether a variable is locked and handles as necessary
    private void handleOperation(Transaction T, Instruction instruction, String type) {

        String transactionID = "T" + T.getID().toString();
        ArrayList<Site> checkSites = new ArrayList<>();
        Integer failCheck;

        Integer variable = instruction.getVariable();
        Integer value;

        // Getting a count of failed sites for later checking against what is
        // not locked
        if(variable % 2 == 0) {
            checkSites = sites;
            failCheck = MAX_SITES - getFailCount();
        } else {
            Integer siteID = 1 + variable % 10;
            Site site = sites.get(siteID - 1);
            checkSites.add(site);
            failCheck = (site.isRunning()) ? 1 : 0;
        }

        // Counts up the number locked
        int not_locked = 0;
        for(Site site: checkSites){
            not_locked += (!site.isVariableLocked(variable) && site.isRunning()) ? 1 : 0;
        }

        // If the number of variables that are not locked is greater than or equal to the
        // failCheck then we can lock
        if(not_locked >= failCheck) {
            for(Site site: sites){
                if(site.isRunning()) {
                    site.lockVariable(T, variable, type);
                }
            }

            T.addLockedVariable(variable);

            // Handles the printing of the read / write variable
            String text;

            if(type.equals("R")) {
                value = getRandomSite(variable).getVariableData("x" + variable);
                System.out.println(transactionID +" read " + variable + ": " + value.toString());
            } else {
                value = instruction.getValue();
                if(checkSites.size() == 1) {
                    text = " at Site " + sites.get(0).getSiteNum();
                } else {
                    text = " to all sites ";
                }
                System.out.println(transactionID +" wrote to x" + variable.toString() + text
                        + ": " + value.toString());
            }
          // This needs to be implemented
        } else if(sites.size() == failCheck || deadLockCheck()) {
            abort(T);
          // Since the variable is locked or cannot be accessed,
          // add to lock queue
        } else {
            for(Site site: sites){
                if(site.isRunning()) {
                    site.addToLockQueue(variable, T);
                }
            }
        }
    }

    // TODO: Need to handle the case when the variable is odd and site is down?
    private Site getRandomSite(Integer index) {

        Random randomGenerator = new Random();
        Integer siteID;
        Site site;

        if(index % 2 == 0) {
            site = sites.get(randomGenerator.nextInt(9));
            while(!site.isRunning()){
                siteID = randomGenerator.nextInt(9);
                site = sites.get(siteID - 1);
            }
        } else {
            siteID = 1 + index % 10;
            site = sites.get(siteID - 1);
        }
        return site;
    }

//    public void processBlockedInstruction(Transaction T) {
//        String instruction = T.getCurrentInstruction();
//        Parser parser = new Parser();
//
//        String[] instruction_split = parser.parseInstruction(instruction);
//        Integer index = ParseInt(instruction_split[2]);
//
//        switch (instruction_split[0]) {
//            case "W":
//                Integer value = Integer.parseInt(instruction_split[3]);
//                write(T, index, value, TM.getTime());
//                break;
//            case "R":
//                index = ParseInt(instruction_split[2]);
//                read(T, index, TM.getTime());
//                break;
//        }
//    }

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
        sites.get(siteID).setSiteState(false);
        sites.get(siteID).clearLocktable();
        failedSites[siteID] = 1;
        failedSiteCount++;
    }

    public Integer getFailCount() {
        return failedSiteCount;
    }

    // Handles recovering a site
    public void recover(Instruction instruction) {
        Integer siteID = instruction.getID();
        sites.get(siteID).setSiteState(true);
        failedSites[siteID] = 0;
        failedSiteCount--;
    }

    public void abort(Transaction T) {

    }

    public Boolean deadLockCheck() {
        return false;
    }

    // Ends a transaction
    // TODO: This needs to also update the value
    public void end(Transaction T) {

        ArrayList<Site> sites = this.sites;
        Queue<Integer> variables = T.getVariablesLocked();

        while(!variables.isEmpty()) {
            Integer variable = variables.remove();

            if(variable % 2 == 0) {
                for(Site site: sites) {
                    if(site.isRunning()) {
                        site.unlockVariable(T, variable);
                    }
                }
            } else {
                Integer site_no = 1 + variable % 10;
                Site site = sites.get(site_no);
                site.unlockVariable(T, variable);
            }
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
