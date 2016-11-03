package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.annotations.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.fork.ForkConfig;
import org.jgroups.fork.ForkProtocol;
import org.jgroups.fork.ForkProtocolStack;
import org.jgroups.fork.UnknownForkHandler;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Bits;
import org.jgroups.util.*;
import org.w3c.dom.Node;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * The FORK protocol; multiplexes messages to different forks in a stack (https://issues.jboss.org/browse/JGRP-1613).
 * See doc/design/FORK.txt for details
 * @author Bela Ban
 * @since  3.4
 */
@XmlInclude(schema="fork-stacks.xsd",type=XmlInclude.Type.IMPORT,namespace="fork",alias="fork")
@XmlElement(name="fork-stacks",type="fork:ForkStacksType")
@MBean(description="Implementation of FORK protocol")
public class FORK extends Protocol {
    public static short ID=ClassConfigurator.getProtocolId(FORK.class);

    @Property(description="Points to an XML file defining the fork-stacks, which will be created at initialization. " +
      "Ignored if null")
    protected String config;

    @Property(description="If enabled, state transfer events will be processed, else they will be passed up")
    protected boolean process_state_events=true;

    private UnknownForkHandler unknownForkHandler = new UnknownForkHandler() {
        @Override
        public Object handleUnknownForkStack(Message message, String forkStackId) {
            log.warn("%s: fork-stack for id=%s not found; discarding message", local_addr, forkStackId);
            return null;
        }

        @Override
        public Object handleUnknownForkChannel(Message message, String forkChannelId) {
            log.warn("%s: fork-channel for id=%s not found; discarding message", local_addr, forkChannelId);
            return null;
        }
    };

    // mappings between fork-stack-ids and fork-stacks (bottom-most protocol)
    protected final ConcurrentMap<String,Protocol> fork_stacks=new ConcurrentHashMap<>();

    protected Address local_addr;

    public void setUnknownForkHandler(UnknownForkHandler unknownForkHandler) {
        this.unknownForkHandler = unknownForkHandler;
    }

    public UnknownForkHandler getUnknownForkHandler() {
        return this.unknownForkHandler;
    }

    public Protocol get(String fork_stack_id)                        {return fork_stacks.get(fork_stack_id);}
    public Protocol putIfAbsent(String fork_stack_id, Protocol prot) {return fork_stacks.put(fork_stack_id, prot);}
    public void     remove(String fork_stack_id)                     {fork_stacks.remove(fork_stack_id);}

    @ManagedAttribute(description="Number of fork-stacks")
    public int getForkStacks() {return fork_stacks.size();}

    public static ForkProtocolStack getForkStack(Protocol prot) {
        while(prot != null && !(prot instanceof ForkProtocolStack))
            prot=prot.getUpProtocol();
        return prot instanceof ForkProtocolStack? (ForkProtocolStack)prot : null;
    }

    public void init() throws Exception {
        super.init();
        if(config != null)
            createForkStacks(config);
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                for(Protocol bottom: fork_stacks.values())
                    bottom.up(evt);
                break;

            case Event.STATE_TRANSFER_OUTPUTSTREAM:
                if(!process_state_events)
                    break;
                getStateFromMainAndForkChannels(evt);
                return null;

            case Event.STATE_TRANSFER_INPUTSTREAM:
                if(!process_state_events)
                    break;
                setStateInMainAndForkChannels(evt.getArg());
                return null;
        }
        return up_prot.up(evt);
    }

    public Object up(Message msg) {
        ForkHeader hdr=msg.getHeader(id);
        if(hdr == null)
            return up_prot.up(msg);
        if(hdr.fork_stack_id == null)
            throw new IllegalArgumentException("header has a null fork_stack_id");
        Protocol bottom_prot=get(hdr.fork_stack_id);
        return bottom_prot != null? bottom_prot.up(msg) : this.unknownForkHandler.handleUnknownForkStack(msg, hdr.fork_stack_id);
    }

    public void up(MessageBatch batch) {
        // Sort fork messages by fork-stack-id
        Map<String,List<Message>> map=new HashMap<>();
        for(Message msg: batch) {
            ForkHeader hdr=msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                List<Message> list=map.get(hdr.fork_stack_id);
                if(list == null) {
                    list=new ArrayList<>();
                    map.put(hdr.fork_stack_id, list);
                }
                list.add(msg);
            }
        }

        // Now pass fork messages up, batched by fork-stack-id
        for(Map.Entry<String,List<Message>> entry: map.entrySet()) {
            String fork_stack_id=entry.getKey();
            List<Message> list=entry.getValue();
            Protocol bottom_prot=get(fork_stack_id);
            if(bottom_prot == null)
                continue;
            MessageBatch mb=new MessageBatch(batch.dest(), batch.sender(), batch.clusterName(), batch.multicast(), list);
            try {
                bottom_prot.up(mb);
            }
            catch(Throwable t) {
                log.error(Util.getMessage("FailedPassingUpBatch"), t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void getStateFromMainAndForkChannels(Event evt) {
        final OutputStream out=evt.getArg();

        try(DataOutputStream dos=new DataOutputStream(out)) {
            getStateFrom(null, up_prot, null, null, dos);

            // now fetch state from all fork channels
            for(Map.Entry<String,Protocol> entry: fork_stacks.entrySet()) {
                String stack_name=entry.getKey();
                Protocol prot=entry.getValue();
                ForkProtocolStack fork_stack=getForkStack(prot);
                for(Map.Entry<String,JChannel> en: fork_stack.getForkChannels().entrySet()) {
                    String fc_name=en.getKey();
                    JChannel fc=en.getValue();
                    getStateFrom(fc, null, stack_name, fc_name, dos);
                }
            }
        }
        catch(Throwable ex) {
            log.error("%s: failed fetching state from main channel", local_addr, ex);
        }
    }


    protected void getStateFrom(JChannel channel, Protocol prot, String stack, String ch, DataOutputStream out) throws Exception {
        ByteArrayDataOutputStream output=new ByteArrayDataOutputStream(1024);
        OutputStreamAdapter out_ad=new OutputStreamAdapter(output);
        Event evt=new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, out_ad);
        if(channel != null)
            channel.up(evt);
        else
            prot.up(evt);
        int len=output.position();
        if(len > 0) {
            Bits.writeString(stack, out);
            Bits.writeString(ch, out);
            out.writeInt(len);
            out.write(output.buffer(), 0, len);
            log.trace("%s: fetched %d bytes from %s:%s", local_addr, len, stack, ch);
        }
    }

    protected void setStateInMainAndForkChannels(InputStream in) {
        try(DataInputStream input=new DataInputStream(in)) {
            for(;;) {
                String stack_name=Bits.readString(input);
                String ch_name=Bits.readString(input);
                int len=input.readInt();
                if(len > 0) {
                    byte[] data=new byte[len];
                    in.read(data, 0, len);
                    ByteArrayInputStream tmp=new ByteArrayInputStream(data, 0, len);
                    if(stack_name == null && ch_name == null)
                        up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, tmp));
                    else {
                        Protocol prot=fork_stacks.get(stack_name);
                        if(prot == null) {
                            log.warn("%s: fork stack %s not found, dropping state for %s:%s", local_addr, stack_name, stack_name, ch_name);
                            continue;
                        }
                        ForkProtocolStack fork_stack=getForkStack(prot);
                        JChannel fork_ch=fork_stack.get(ch_name);
                        if(fork_ch == null) {
                            log.warn("%s: fork channel %s not found, dropping state for %s:%s", local_addr, ch_name, stack_name, ch_name);
                            continue;
                        }
                        fork_ch.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, tmp));
                    }
                }
            }
        }
        catch(EOFException eof) {
        }
        catch(Throwable ex) {
            log.error("%s: failed setting state in main channel", local_addr, ex);
        }
    }


    protected void createForkStacks(String config) throws Exception {
        InputStream in=getForkStream(config);
        if(in == null)
            throw new FileNotFoundException("fork stacks config " + config + " not found");
        Map<String,List<ProtocolConfiguration>> protocols=ForkConfig.parse(in);
        createForkStacks(protocols);
    }

    protected void createForkStacks(Map<String,List<ProtocolConfiguration>> protocols) throws Exception {
        for(Map.Entry<String,List<ProtocolConfiguration>> entry: protocols.entrySet()) {
            String fork_stack_id=entry.getKey();
            if(get(fork_stack_id) != null)
                continue;

            List<Protocol> prots=createProtocols(null,entry.getValue());
            createForkStack(fork_stack_id, prots, false);
        }
    }

    public void parse(Node node) throws Exception {
        Map<String,List<ProtocolConfiguration>> protocols=ForkConfig.parse(node);
        createForkStacks(protocols);
    }

    /**
     * Returns the fork stack for fork_stack_id (if exitstent), or creates a new fork-stack from protocols and adds it
     * into the hashmap of fork-stack (key is fork_stack_id).
     * Method init() will be called on each protocol, from bottom to top.
     * @param fork_stack_id The key under which the new fork-stack should be added to the fork-stacks hashmap
     * @param protocols A list of protocols from <em>bottom to top</em> to be inserted. They will be sandwiched
     *                  between ForkProtocolStack (top) and ForkProtocol (bottom). The list can be empty (or null) in
     *                  which case we won't create any protocols, but still have a separate fork-stack inserted.
     * @param initialize If false, the ref count 'inits' will not get incremented and init() won't be called. This is
     *                   needed when creating a fork stack from an XML config inside of the FORK protocol. The protocols
     *                   in the fork stack will only get initialized on the first ForkChannel creation
     * @return The new {@link ForkProtocolStack}, or the existing stack (if present)
     */
    public synchronized ProtocolStack createForkStack(String fork_stack_id, List<Protocol> protocols, boolean initialize) throws Exception {
        Protocol bottom;
        if((bottom=get(fork_stack_id)) != null) {
            ForkProtocolStack retval=getForkStack(bottom);
            return initialize? retval.incrInits() : retval;
        }

        List<Protocol> prots=new ArrayList<>();
        prots.add(bottom=new ForkProtocol(fork_stack_id).setDownProtocol(this)); // add a ForkProtocol as bottom protocol
        if(protocols != null)
            prots.addAll(protocols);

        ForkProtocolStack fork_stack=(ForkProtocolStack)new ForkProtocolStack(getUnknownForkHandler(), prots, fork_stack_id).setChannel(this.stack.getChannel());
        fork_stack.init();
        if(initialize)
            fork_stack.incrInits();
        fork_stacks.put(fork_stack_id, bottom);
        return fork_stack;
    }



    /** Creates a fork-stack from the configuration, initializes all protocols (setting values),
     * sets the protocol stack as top protocol, connects the protocols and calls init() on them. Returns
     * the protocols in a list, from bottom to top */
    protected static List<Protocol> createProtocols(ProtocolStack stack, List<ProtocolConfiguration> protocol_configs) throws Exception {
        return Configurator.createProtocols(protocol_configs,stack);
    }

    public static InputStream getForkStream(String config) throws IOException {
        InputStream configStream = null;

        try {
            configStream=new FileInputStream(config);
        }
        catch(FileNotFoundException | AccessControlException fnfe) { // catching ACE fixes http://jira.jboss.com/jira/browse/JGRP-94
        }

        // Check to see if the properties string is a URL.
        if(configStream == null) {
            try {
                configStream=new URL(config).openStream();
            }
            catch (MalformedURLException mre) {
            }
        }

        // Check to see if the properties string is the name of a resource, e.g. udp.xml.
        if(configStream == null)
            configStream=Util.getResourceAsStream(config, ConfiguratorFactory.class);
        return configStream;
    }


    public static class ForkHeader extends Header {
        protected String fork_stack_id, fork_channel_id;

        public ForkHeader() {
        }

        public ForkHeader(String fork_stack_id, String fork_channel_id) {
            this.fork_stack_id=fork_stack_id;
            this.fork_channel_id=fork_channel_id;
        }
        public short getMagicId() {return 83;}
        public Supplier<? extends Header> create() {return ForkHeader::new;}

        public String getForkStackId() {
            return fork_stack_id;
        }

        public void setForkStackId(String fork_stack_id) {
            this.fork_stack_id=fork_stack_id;
        }

        public String getForkChannelId() {
            return fork_channel_id;
        }

        public void setForkChannelId(String fork_channel_id) {
            this.fork_channel_id=fork_channel_id;
        }

        public int serializedSize() {return Util.size(fork_stack_id) + Util.size(fork_channel_id);}

        public void writeTo(DataOutput out) throws Exception {
            Bits.writeString(fork_stack_id,out);
            Bits.writeString(fork_channel_id,out);
        }

        public void readFrom(DataInput in) throws Exception {
            fork_stack_id=Bits.readString(in);
            fork_channel_id=Bits.readString(in);
        }

        public String toString() {return fork_stack_id + ":" + fork_channel_id;}
    }
}
