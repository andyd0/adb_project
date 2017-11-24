package adb_project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class DM {

    private final Integer MAX_SITES = 10;
    private final Integer MAX_VARIABLES = 20;

    private ArrayList<Site> sites = new ArrayList<>(MAX_SITES);
    HashMap<Integer, ArrayList<Integer>> variableHistory =  new HashMap<>();

    public DM() {
        this.sites = initializeSites();
        this.variableHistory = initializeHistory();
    }

    public void write(Transaction T, Integer index, Integer value, Integer time) {

        String transactionID = "T" + T.getID().toString();
        ArrayList<Site> sites = this.sites;
        ArrayList<Integer> history = (ArrayList<Integer>) this.variableHistory.get(time - 1).clone();

        if(index % 2 == 0) {
            int not_locked = 0;
            for(Site site: sites){
                not_locked += (!site.isVariableLocked(index.toString())) ? 1 : 0;
            }
            if(not_locked == MAX_SITES){
                for(Site site: sites){
                    site.lockVariable(T, index.toString(), "W");
                    site.updateVariable(index.toString(), value);
                }
                history.set(index, value);
                System.out.println(transactionID +" wrote to x" + index.toString() + " to all sites "
                                   + ": " + value.toString());
            }

        } else {
            Integer site_no = 1 + index % 10;
            Site site = sites.get(site_no);
            if(!site.isVariableLocked(index.toString())) {
                site.lockVariable(T, index.toString(), "W");
                site.updateVariable(index.toString(), value);
                history.set(index, value);
                System.out.println(transactionID +" wrote to x" + index.toString() + " at Site " + site_no.toString()
                                   + ": " + value.toString());
            }
        }
        this.variableHistory.put(time, history);
    }

    public void read(Transaction T, Integer index, Integer time) {

        String transactionID = "T" + T.getID().toString();
        Random randomGenerator = new Random();
        Integer siteID = randomGenerator.nextInt(9);
        Integer value;

        Site site = this.sites.get(siteID);

        while(!site.isRunning()){
            siteID = randomGenerator.nextInt(9);
            site = this.sites.get(siteID);
        }

        if(T.isReadOnly()) {
            value = this.variableHistory.get(T.getStartTime() - 1).get(index - 1);
        } else {
            value = site.getVariableData(index.toString());
        }

        System.out.println(transactionID +" read x" + index.toString() + ": " + value.toString());
        addToHistory(time);
    }

    public void fail(Integer siteID) {
    }

    public void recover(Integer siteID) {
    }

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

    public void dump(String[] instruction_split) {
        ArrayList<Site> sites = this.sites;
        if(instruction_split.length > 1){
            System.out.println("For now");
        } else {
            for(Site site: sites) {
                System.out.println(site.toString());
            }

        }
    }

    public void addToHistory(Integer time) {
        ArrayList<Integer> history = (ArrayList<Integer>) this.variableHistory.get(time - 1).clone();
        this.variableHistory.put(time, history);
    }

    private Integer ParseInt(String T) {
        return Integer.parseInt(T.replaceAll("\\D+",""));
    }
}
