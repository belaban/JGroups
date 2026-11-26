package org.jgroups.admin;

import org.jgroups.Address;
import org.jgroups.ObjectMessage;
import org.jgroups.util.ResponseCollector;

import java.util.List;
import java.util.Map;

/**
 * Component which is used on the sender side. Sends a PREPARE command (string) to all members, waits for the results.
 * If a timeout occurred, or some of the results are "FAIL", then a ROLLBACK command is sent, else (if set) a
 * COMMIT command.<br/>
 * Returns true or false accordingly.
 * @author Bela Ban
 * @since  5.6.0
 */
public class TwoPhaseCommit extends Phase {
    protected String                    rollback, commit;
    protected ResponseCollector<String> rsps;


    public String                    commit()                             {return commit;}
    public void                      commit(String commit)                {this.commit=commit;}
    public String                    rollback()                           {return rollback;}
    public void                      rollback(String rollback)            {this.rollback=rollback;}
    public ResponseCollector<String> rsps()                               {return rsps;}
    public void                      rsps(ResponseCollector<String> rsps) {this.rsps=rsps;}

    public TwoPhaseCommit(AdminHandler admin, String key, Address dest, String name,
                          String prepare, String rollback, String commit, long timeout) {
        super(admin, key, dest, name, prepare, timeout);
        this.rollback=rollback;
        this.commit=commit;
        this.rsps=new ResponseCollector<>(dest == null? admin.channel().view().getMembers() : List.of(dest));
    }

    public boolean execute() {
        super.execute();
        boolean result=rsps.waitForAllResponses(timeout);

        // check responses for "FAIL: " instances
        if(result) // received responses from all members
            result=checkAllResponsesOK(rsps);
        if(result == false && rollback != null) {
            // send an async request and don't wait for responses
            admin.sendRequest(key, new ObjectMessage(dest, rollback));
            return false;
        }
        if(commit != null)
            admin.sendRequest(key, new ObjectMessage(dest, commit));
        return true;
    }

    public void performRollback() {
        if(rollback != null)
            admin.sendRequest(key, new ObjectMessage(dest, rollback));
    }

    public void handleResponse(Address sender, String rsp) {
        rsps.add(sender, rsp);
    }

    @Override
    public String toString() {
        return String.format("%s (%s): rsps: %s", name, admin.channel().address(), rsps);
    }

    private static boolean checkAllResponsesOK(ResponseCollector<String> rsps) {
        Map<Address,String> results=rsps.getResults();
        return results.values().stream().allMatch(r -> r.startsWith(AdminHandler.OK));
    }
}
