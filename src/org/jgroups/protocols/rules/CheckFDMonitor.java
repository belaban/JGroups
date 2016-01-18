package org.jgroups.protocols.rules;

import org.jgroups.View;
import org.jgroups.protocols.FD;

/**
 * Rule which checks if the FD monitor is running when we have more than 1 member. If not, starts it.
 * The rule uninstalls itself if no FD protocol is found.
 * @author Bela Ban
 * @since  3.3
 */
public class CheckFDMonitor extends AbstractRule {
    protected FD fd;

    public String name() {
        return getClass().getSimpleName();
    }

    public String description() {
        return "Starts FD.Monitor if membership > 1 and monitor isn't running";
    }

    public void init() {
        super.init();
        fd=(FD)sv.getProtocolStack().findProtocol(FD.class);
        if(fd == null) {
            log.info("FD was not found, uninstalling myself (" + getClass().getSimpleName() + ")");
            sv.uninstallRule(getClass().getSimpleName());
        }
    }

    public boolean eval() {
        return sv.getView() != null && sv.getView().size() > 1 && !fd.isMonitorRunning();
    }

    public String condition() {
        View view=sv.getView();
        return "Membership is " + (view != null? view.size() : "n/a") + ", FD.Monitor running=" + fd.isMonitorRunning();
    }

    public void trigger() throws Throwable {
        System.out.println(sv.getLocalAddress() + ": starting failure detection");
        fd.startFailureDetection();
    }

    public String toString() {
        return CheckFDMonitor.class.getSimpleName() + ": " + condition();
    }
}
