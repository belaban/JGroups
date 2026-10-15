package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.DiagnosticsHandler;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link AdminHandler} which invokes all {@link org.jgroups.stack.DiagnosticsHandler.ProbeHandler}
 * instances in the instance and returns the collected responses as strings
 * @author Bela Ban
 * @since  5.6.0
 */
public class ProbeAdminHandler extends AdminHandler {
    protected final AtomicInteger cnt=new AtomicInteger();
    public static final String KEY="probe";

    public ProbeAdminHandler(JChannel ch) {
        super(ch);
    }

    /**
     * Sends a probe command to all members of the current cluster. Example: "probe FRAG2.number"
     * @param dest The destination address; null to send the command to all members
     * @param cmd The probe command
     */
    public void sendProbeRequest(Address dest, String cmd) {
        cnt.set(0);
        sendRequest(KEY, new ObjectMessage(dest, cmd));
    }

    @Override
    public void handleRequest(Message msg, AdminHeader ignored) {
        TP tp=ch.stack().getTransport();
        DiagnosticsHandler diag=tp.getDiagnosticsHandler();
        String req=msg.getObject();
        PhysicalAddress sender=tp.getPhysicalAddressFromCache(msg.src());
        SocketAddress sa=sender == null? null : sender.getSocketAddress();
        diag.handleDiagnosticProbe(sa, req, (addr,r) -> {
            Message rsp=new ObjectMessage(msg.src(), r);
            sendResponse(KEY, rsp);
        });
    }

    @Override
    public void handleResponse(Message msg, AdminHeader ignored) {
        int count=cnt.incrementAndGet();
        System.out.printf("#%d: %s\n", count, msg.getObject());
    }
}
