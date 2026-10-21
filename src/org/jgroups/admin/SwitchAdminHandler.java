package org.jgroups.admin;

import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.VERSION;
import org.jgroups.stack.AddressGenerator;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Manages the switch from one protocol configuration to another one (same JGroups version).
 * Details in https://redhat.atlassian.net/browse/JGRP-3043
 * @author Bela Ban
 * @since  5.6.0
 */
public class SwitchAdminHandler extends AdminHandler {
    public static final String     KEY="switch";
    public static final String     INSERT_VERSION="insert-version"; //  | version |
    public static final String     ROLLBACK_INSERT_VERSION="rollback-insert-version";
    // | version=<version> | cfg=<config (String)> |
    public static final String     CREATE_NEW_STACK="create-new-stack";
    public static final String     ROLLBACK_NEW_STACK="rollback-new-stack";

    public static final String     PREPARE_CONNECT="prepare-connect"; // | expected cluster size |
    public static final String     ROLLBACK_CONNECT="rollback-connect";
    public static final String     COMMIT_CONNECT="commit-connect";

    public static final String     VERSION="version=";
    public static final String     CFG="cfg=";
    protected SwitchListener       listener;
    protected long                 timeout=10000; // ms
    protected List<TwoPhaseCommit> phases=new ArrayList<>(), completed_phases=new ArrayList<>();
    protected JChannel             new_channel; // used in create-new-stack


    public SwitchAdminHandler(JChannel ch) {
        super(ch);
    }

    public SwitchAdminHandler listener(SwitchListener l) {this.listener=l; return this;}
    public SwitchAdminHandler timeout(long t)            {this.timeout=t; return this;}

    /** Triggers a switch to a new configuration. Returns true when the switch was successful, else false.
     * False means that all cluster members are still running the current configuration. */
    public boolean switchToNewConfiguration(String file) {
        String new_config=null;
        // 1. Read the contents of the file
        try {
            new_config=Util.readFile(file);
            listener.statusChange(0.0, String.format("%s read %s", OK, file));
        }
        catch(FileNotFoundException e) {
            ch.log().error("%s: failed reading file %s: %s", ch.address(), file, e);
            return false;
        }

        List<Address> members=ch.view().getMembers();
        Address       local=ch.address();

        // 1. Insert VERSION(1) into the old stack
        TwoPhaseCommit ph=new TwoPhaseCommit(KEY, "insert VERSION", this, null, members,
                                             String.format("%s %d", INSERT_VERSION, 1),
                                             ROLLBACK_INSERT_VERSION, null, 3000);
        phases.add(ph);

        // 2. Create the new channel in all cluster members, insert VERSION(2)
        ph=new TwoPhaseCommit(KEY, "create new channel", this, null, members,
                              createNewStack(new_config, 2),
                              ROLLBACK_NEW_STACK, null, 3000);
        phases.add(ph);

        // 3. Connect the first (=current) member
        ph=new TwoPhaseCommit(KEY, "connect first member", this, local, List.of(local),
                              String.format("%s %d", PREPARE_CONNECT, 1),
                              ROLLBACK_CONNECT, COMMIT_CONNECT, 5000);
        phases.add(ph);

        // 4. Connect the new channel in all other members (the current member will skip this)
        ph=new TwoPhaseCommit(KEY, "connect all members", this, null, members,
                              String.format("%s %d", PREPARE_CONNECT, members.size()),
                              ROLLBACK_CONNECT, COMMIT_CONNECT, 10000);
        phases.add(ph);

        int incr=100 / phases.size(), p=0;
        while(!phases.isEmpty()) {
            TwoPhaseCommit phase=phases.get(0);
            long start=System.nanoTime();
            boolean success=phase.execute();
            long time=System.nanoTime()-start;
            p+=incr;
            if(success) {
                listener.statusChange(p, String.format("OK: %s completed successfully in %s, results:\n%s",
                                                       phase.name(), Util.printTime(time), print(phase.rsps())));
                TwoPhaseCommit processed=phases.remove(0);
                completed_phases.add(0, processed);
            }
            else {
                listener.statusChange(p, String.format("FAIL: %s failed in %s, results:\n%s",
                                                       phase.name(), Util.printTime(time), print(phase.rsps())));
                rollbackCompletedPhases();
                return false;
            }
        }
        return true;
    }

    @Override
    public void handleRequest(Message msg, AdminHeader hdr) {
        String s=msg.getObject();
        int index=s.indexOf(' ');
        String cmd=index == -1? s : s.substring(0, index);

        ch.log().debug("%s received command %s", ch.address(), cmd);
        switch(cmd) {
            case INSERT_VERSION:
                try {
                    int ver=Integer.parseInt(s.substring(INSERT_VERSION.length()).trim());
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
                    sendResponse(KEY, new ObjectMessage(msg.src(), String.format("%s %s", FAIL, e.getMessage())));
                }
                break;
            case ROLLBACK_INSERT_VERSION:
                ProtocolStack stack=ch.stack();
                stack.removeProtocol(VERSION.class);
                break;
            case CREATE_NEW_STACK:
                if(new_channel != null && new_channel.isOpen()) {
                    sendResponse(KEY, new ObjectMessage(msg.src(), "OK: new channel already created"));
                    return;
                }
                handleCreateNewStack(s.substring(cmd.length()), msg.src());
                break;
            case ROLLBACK_NEW_STACK:
                if(new_channel != null) {
                    Util.close(new_channel);
                    new_channel=null;
                }
                break;
            case PREPARE_CONNECT:
                if(new_channel != null && new_channel.isConnected()) {
                    sendResponse(KEY, new ObjectMessage(msg.src(), "OK: new channel already connected"));
                    return;
                }
                handleConnect(s.substring(cmd.length()), msg.src());
                break;
            case ROLLBACK_CONNECT:
                Util.close(new_channel);
                break;
            case COMMIT_CONNECT:
                // 1. Make channel use the new stack, delete the old stack
                if(new_channel == null)
                    break;
                ProtocolStack old_stack=ch.stack();
                ch.stack(new_channel.stack());
                new_channel.stack(old_stack);
                Util.close(new_channel);
                new_channel=null;
                DiagnosticsHandler.ProbeHandler ph=new JChannelProbeHandler(ch);
                ch.probeHandler(ph);
                TP tp=ch.stack().getTransport();
                tp.unregisterProbeHandler(ph.getClass());
                tp.registerProbeHandler(ph);
                break;
            default:
                ch.log().warn("%s: request %s not known", ch.address(), cmd);
        }
    }

    @Override
    public void handleResponse(Message msg, AdminHeader hdr) {
        String s=msg.getObject();
        TwoPhaseCommit phase=phases.isEmpty()? null : phases.get(0);
        if(phase != null)
            phase.handleResponse(msg.src(), s);
    }

    /**
     * Gets the version and config. Creates a new protocol stack with the old channel's address injected into it. Adds
     * VERSION (with the received version) above the transport. Then the up_handler and receiver of the old channel
     * are nulled (and saved for later).<br/>
     * On a rollback, the new stack is destroyed and the up_handler and receiver of the old channel are re-inserted.
     * <br/>
     * On a commit, the new protocol stack is set in the existing channel and up_handler and receiver are re-inserted.
     * The old stack is then destroyed.
     */
    public boolean handleCreateNewStack(String s, Address sender) {
        String cfg=null;
        int    version;
        try {
            // parse version=X
            int len=VERSION.length();
            int index=s.indexOf(VERSION)+len, end=s.indexOf(' ', len+1);
            String str=s.substring(index, end);
            version=Integer.parseInt(str.trim());
            s=s.substring(end+1);

            // now parse cfg=<.../>
            len=CFG.length();
            index=s.indexOf(CFG)+len;
            cfg=s.substring(index);

            // Create the new stack
            new_channel=createNewChannel(version, cfg, ch.name());
            ch.log().debug("%s: new channel created successfully: %s", ch.address(), new_channel);
            String f=String.format("%s new channel created successfully: %s", OK, new_channel);
            sendResponse(KEY, new ObjectMessage(sender, f));
        }
        catch(Exception e) {
            String f=String.format("%s %s", FAIL, e.getMessage());
            ch.log().error(f);
            sendResponse(KEY, new ObjectMessage(sender, f));
            Util.close(new_channel);
            return false;
        }
        return false;
    }

    /**
     * Connects the channel and waits for expected_members. Returns true if cluster size == expected_members, false
     * oherwise
     * @param sender
     */
    public boolean handleConnect(String s, Address sender) {
        AddressGenerator gen=ch::address;
        String ok="%s connected new channel successfully, %d members: %s (expected: %d)",
          fail="%s failed connecting new channel, %d members: %s (expected: %d)";
        int expected_members=Integer.parseInt(s.trim());
        try {
            // Inject the *same* address as the old channel into the new channel. This prevents rebalancing in
            // Infinispan when switching to the new cluster
            final JChannel tmp=new_channel;
            tmp.addAddressGenerator(gen);
            tmp.connect(ch.clusterName());
            BooleanSupplier cond=() -> tmp.view() != null && tmp.view().size() == expected_members;
            boolean success=Util.waitUntilTrue(timeout, 100, cond);
            View v=tmp.view();
            String f;
            if(success) {
                f=String.format(ok, OK, v.size(), v, expected_members);
                ch.log().debug(f);
                sendResponse(KEY, new ObjectMessage(sender, f));
            }
            else {
                f=String.format(fail, FAIL, v.size(), v, expected_members);
                ch.log().error(f);
                sendResponse(KEY, new ObjectMessage(sender, f));
                Util.close(new_channel);
            }
        }
        catch(Exception e) {
            String f=String.format("%s %s", FAIL, e.getMessage());
            ch.log().error(f);
            sendResponse(KEY, new ObjectMessage(sender, f));
            Util.close(new_channel);
            return false;
        }
        finally {
            if(new_channel != null)
                new_channel.removeAddressGenerator(gen);
        }
        return false;
    }

    protected void notifyListener(double percentage, String msg) {
        if(listener != null)
            listener.statusChange(percentage, msg);
    }

    protected static String print(ResponseCollector<String> r) {
        Map<Address, String> results=r.getResults();
        return results.entrySet().stream()
          .map(e -> String.format("     %s: %s", e.getKey(), e.getValue() == null? "<timed out>" : e.getValue()))
          .collect(Collectors.joining("\n"));
    }

    protected static String createNewStack(String new_config, int version) {
        return String.format("%s version=%d cfg=%s", CREATE_NEW_STACK, version, new_config);
    }

    protected static JChannel createNewChannel(int version, String config, String name) throws Exception {
        try(ByteArrayDataInputStream in=new ByteArrayDataInputStream(config.getBytes())) {
            JChannel new_ch=new JChannel(in).name(name + "-new");
            VERSION v=new VERSION().version(version);
            new_ch.stack().insertProtocol(v, ProtocolStack.Position.ABOVE, TP.class);
            return new_ch;
        }
    }

    /** Rolls back already completed phases if a later phase fails (latest completed first) */
    protected void rollbackCompletedPhases() {
        for(TwoPhaseCommit phase: completed_phases)
            phase.performRollback();
        completed_phases.clear();
    }



}
