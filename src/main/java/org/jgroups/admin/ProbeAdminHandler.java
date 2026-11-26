package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.util.ResponseCollector;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implementation of {@link AdminHandler} which invokes all {@link org.jgroups.stack.DiagnosticsHandler.ProbeHandler}
 * instances in the instance and returns the collected responses as strings
 * @author Bela Ban
 * @since  5.6.0
 */
public class ProbeAdminHandler extends AdminHandler {
    protected final AtomicInteger             cnt=new AtomicInteger();
    public static final String                KEY="probe";
    protected long                            timeout=5000;
    protected final ResponseCollector<String> rsp_coll=new ResponseCollector<>();


    public ProbeAdminHandler(JChannel ch) {
        super(ch);
    }

    public long timeout()       {return timeout;}
    public void timeout(long t) {this.timeout=t;}

    @Override
    public String executeRequest(String req) {
        return sendProbeRequest(null, req);
    }

    /**
     * Sends a probe command to all members of the current cluster. Example: "probe FRAG2.number"
     * @param dest The destination address; null to send the command to all members
     * @param cmd The probe command
     */
    public String sendProbeRequest(Address dest, String cmd) {
        cnt.set(0);
        rsp_coll.reset(dest == null? ch.view().getMembers() : List.of(dest));
        sendRequest(KEY, new ObjectMessage(dest, cmd));
        rsp_coll.waitForAllResponses(timeout);
        return rsp_coll.getResults().entrySet().stream()
          .map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
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
        String s=msg.getObject();
        System.out.printf("#%d: %s\n", count, s);
        rsp_coll.add(msg.src(), s);
    }
}
