package org.jgroups.admin;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.VERSION;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages the switch from one protocol configuration to another one (same JGroups version).
 * Details in https://redhat.atlassian.net/browse/JGRP-3043
 * @author Bela Ban
 * @since  5.6.0
 */
public class SwitchAdminHandler extends AdminHandler {
    public static final String     KEY="switch";
    public static final String     INSERT_VERSION="insert-version"; // + version
    public static final String     ROLLBACK_INSERT_VERSION="rollback-insert-version";
    protected SwitchListener       listener;
    protected long                 timeout=10000; // ms
    protected List<TwoPhaseCommit> phases=new ArrayList<>();


    public SwitchAdminHandler(JChannel ch) {
        super(ch);
    }

    public SwitchAdminHandler listener(SwitchListener l) {this.listener=l; return this;}
    public SwitchAdminHandler timeout(long t)            {this.timeout=t; return this;}

    /** Triggers a switch to a new configuration. Returns true when the switch was successful, else false.
     * False means that all cluster members are still running the current configuration. */
    public boolean switchToNewConfiguration(String file) {
        // 1. Read the contents of the file
        try {
            String new_config=Util.readFile(file);
            listener.statusChange(0.0, String.format("read the contents of %s", file));
        }
        catch(FileNotFoundException e) {
            ch.log().error("%s: failed reading file %s: %s", ch.address(), file, e);
            return false;
        }

        List<Address> members=ch.view().getMembers();
        TwoPhaseCommit ph=new TwoPhaseCommit(KEY, "insert VERSION", this, members,
                                             String.format("%s %d", INSERT_VERSION, 1),
                                             ROLLBACK_INSERT_VERSION, null, 3000);
        phases.add(ph);

        int incr=100 / phases.size(), p=0;
        while(!phases.isEmpty()) {
            TwoPhaseCommit phase=phases.get(0);
            boolean success=phase.execute();
            p+=incr;
            if(success) {
                listener.statusChange(p, String.format("OK: %s completed successfully, results:\n%s",
                                                       phase.name(), print(phase.rsps())));
                phases.remove(0);
            }
            else {
                listener.statusChange(p, String.format("FAIL: %s failed, results:\n%s",
                                                       phase.name(), print(phase.rsps())));
                return false;
            }
        }
        return true;
    }

    @Override
    public void handleRequest(Message msg, AdminHeader hdr) {
        String cmd=msg.getObject();
        if(cmd.startsWith(INSERT_VERSION)) {
            try {
                int ver=Integer.parseInt(cmd.substring(INSERT_VERSION.length()).trim());
                ProtocolStack stack=ch.stack();
                VERSION version=stack.findProtocol(VERSION.class);
                if(version == null) {
                    version=new VERSION().version(ver);
                    stack.insertProtocol(version, ProtocolStack.Position.ABOVE, TP.class);
                    sendResponse(KEY, new ObjectMessage(msg.src(), "OK: VERSION inserted successfully"));
                }
                else
                    sendResponse(KEY, new ObjectMessage(msg.src(), "OK: VERSION already present"));
            }
            catch(Exception e) {
                sendResponse(KEY, new ObjectMessage(msg.src(), String.format("FAIL: %s", e.getMessage())));
            }
        }
        else if(cmd.startsWith(ROLLBACK_INSERT_VERSION)) {
            ProtocolStack stack=ch.stack();
            stack.removeProtocol(VERSION.class);
        }
    }

    @Override
    public void handleResponse(Message msg, AdminHeader hdr) {
        String s=msg.getObject();
        TwoPhaseCommit phase=phases.isEmpty()? null : phases.get(0);
        if(phase != null)
            phase.handleResponse(msg.src(), s);
    }

    protected void notifyListener(double percentage, String msg) {
        if(listener != null)
            listener.statusChange(percentage, msg);
    }

    protected static String print(ResponseCollector<String> r) {
        Map<Address, String> results=r.getResults();
        return results.entrySet().stream()
          .map(e -> String.format("   %s: %s", e.getKey(), e.getValue()))
          .collect(Collectors.joining("\n"));
    }

}
