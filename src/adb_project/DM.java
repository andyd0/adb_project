package adb_project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.Random;

public class DM {

    private final Integer MAX_SITES = 10;
    private final Integer MAX_VARIABLES = 20;
    private int failedSiteCount;

    private ArrayList<Site> sites;
    private int[] failedSites;
    HashMap<Integer, ArrayList<Integer>> variableHistory;

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
        variableHistory = initializeHistory();
    }

    public void write(Transaction T, Integer variable, Integer value, Integer time) {

        String transactionID = "T" + T.getID().toString();
        ArrayList<Site> check_sites = new ArrayList<>();
        ArrayList<Integer> history = (ArrayList<Integer>) variableHistory.get(time - 1).clone();
        Integer failCheck;

        if(variable % 2 == 0) {
            check_sites = sites;
            failCheck = MAX_SITES - getFailCount();
        } else {
            Integer site_no = 1 + variable % 10;
            Site site = sites.get(site_no);
            check_sites.add(site);
            failCheck = (site.isRunning()) ? 1 : 0;
        }

        int not_locked = 0;
        for(Site site: check_sites){
            not_locked += (!site.isVariableLocked(variable) &&
                           site.isRunning()) ? 1 : 0;
        }

        if(not_locked >= failCheck) {
            for(Site site: sites){
                if(site.isRunning()) {
                    site.lockVariable(T, variable, "W");
                    site.updateVariable(variable.toString(), value);
                }
            }
            T.addLockedVariable(variable);
            history.set(variable, value);
            String text;

            if(sites.size() == 1) {
                text = " at Site " + sites.get(0).getSiteNum();
            } else {
                text = " to all sites ";
            }

            System.out.println(transactionID +" wrote to x" + variable.toString() + text
                               + ": " + value.toString());

        } else if(sites.size() == failCheck || deadLockCheck()) {
            abort(T);
        } else {
            for(Site site: sites){
                if(site.isRunning()) {
                    site.addToLockQueue(variable, T);
                }
            }
        }

        variableHistory.put(time, history);
    }

    public void read(Transaction T, Integer index, Integer time) {

        String transactionID = "T" + T.getID().toString();
        Random randomGenerator = new Random();
        Integer siteID = randomGenerator.nextInt(9);
        Integer value;

        Site site = sites.get(siteID);

        while(!site.isRunning()){
            siteID = randomGenerator.nextInt(9);
            site = sites.get(siteID);
        }

        if(T.isReadOnly()) {
            value = variableHistory.get(T.getStartTime() - 1).get(index - 1);
        } else {
            value = site.getVariableData(index.toString());
        }

        System.out.println(transactionID +" read x" + index.toString() + ": " + value.toString());
        addToHistory(time);
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

    private ArrayList<Site> initializeSites() {
        ArrayList<Site> sites = new ArrayList<>();
        for (int i = 1; i <= MAX_SITES; i++) {
            Site s = new Site(i);
            sites.add(s);
        }
        return sites;
    }

    private HashMap<Integer, ArrayList<Integer>> initializeHistory() {
        HashMap<Integer, ArrayList<Integer>> variableHistory = new HashMap<>();
        ArrayList<Integer> initial = new ArrayList<>();
        for (int i = 1; i <= MAX_VARIABLES; i++) {
            Integer v = i * 10;
            initial.add(v);
        }
        variableHistory.put(0, initial);
        return variableHistory;
    }

    public void addToHistory(Integer time) {
        ArrayList<Integer> history = (ArrayList<Integer>) variableHistory.get(time - 1).clone();
        variableHistory.put(time, history);
    }

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
