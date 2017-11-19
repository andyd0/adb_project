package adb_project;

import java.util.Hashtable;

public class DM {

    private Hashtable<Integer, Integer> lockTable = new Hashtable<>(10);

    public void LockTable(Transaction T) {

        lockTable.put(T.getID(), T.getVariable());

    }

}
