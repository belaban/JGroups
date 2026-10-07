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
public record TwoPhaseCommit(String key, String name, AdminHandler admin, Address dest, List<Address> targets,
                             String prepare, String rollback, String commit,
                             long timeout, ResponseCollector<String> rsps) {


    public TwoPhaseCommit(String key, String name, AdminHandler admin, Address dest, List<Address> targets,
                                 String prepare, String rollback, String commit,
                                 long timeout) {
        this(key, name, admin, dest, targets, prepare, rollback, commit, timeout, new ResponseCollector<>(targets));
    }

    public boolean execute() {
        admin.sendRequest(key, new ObjectMessage(dest, prepare));
        boolean result=rsps.waitForAllResponses(timeout);

        // check responses for "FAIL: " instances
        if(result) // received responses from all members
            result=checkAllResponsesOK(rsps);
        if(result == false) {
            // send an async request and don't wait for responses
            admin.sendRequest(key, new ObjectMessage(dest, rollback));
            return false;
        }
        if(commit != null)
            admin.sendRequest(key, new ObjectMessage(dest, commit));
        return true;
    }

    public void performRollback() {
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
