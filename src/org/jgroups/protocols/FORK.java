package org.jgroups.protocols;

import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.annotations.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.fork.ForkConfig;
import org.jgroups.fork.ForkProtocol;
import org.jgroups.fork.ForkProtocolStack;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Bits;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
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

    // mappings between fork-stack-ids and fork-stacks (bottom-most protocol)
    protected final ConcurrentMap<String,Protocol> fork_stacks=new ConcurrentHashMap<String,Protocol>();


    public Protocol get(String fork_stack_id)                        {return fork_stacks.get(fork_stack_id);}
    public Protocol putIfAbsent(String fork_stack_id, Protocol prot) {return fork_stacks.put(fork_stack_id, prot);}


    @ManagedAttribute(description="Number of fork-stacks")
    public int getForkStacks() {return fork_stacks.size();}

    public void init() throws Exception {
        super.init();
        if(config != null)
            createForkStacks(config, false);
    }



    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
                Message msg=(Message)evt.getArg();
                ForkHeader hdr=(ForkHeader)msg.getHeader(id);
                if(hdr == null)
                    break;
                if(hdr.fork_stack_id == null)
                    throw new IllegalArgumentException("header has a null fork_stack_id");
                Protocol bottom_prot=get(hdr.fork_stack_id);
                return bottom_prot != null? bottom_prot.up(evt) : null;

            case Event.VIEW_CHANGE:
                for(Protocol bottom: fork_stacks.values())
                    bottom.up(evt);
                break;
        }
        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        // Sort fork messages by fork-stack-id
        Map<String,List<Message>> map=new HashMap<String,List<Message>>();
        for(Message msg: batch) {
            ForkHeader hdr=(ForkHeader)msg.getHeader(id);
            if(hdr != null) {
                batch.remove(msg);
                List<Message> list=map.get(hdr.fork_stack_id);
                if(list == null) {
                    list=new ArrayList<Message>();
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
                log.error("failed passing up batch", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    protected void createForkStacks(String config, boolean replace_existing) throws Exception {
        InputStream in=getForkStream(config);
        if(in == null)
            throw new FileNotFoundException("fork stacks config " + config + " not found");
        Map<String,List<ProtocolConfiguration>> protocols=ForkConfig.parse(in);
        createForkStacks(protocols, replace_existing);
    }

    protected void createForkStacks(Map<String,List<ProtocolConfiguration>> protocols, boolean replace_existing) throws Exception {
        for(Map.Entry<String,List<ProtocolConfiguration>> entry: protocols.entrySet()) {
            String fork_stack_id=entry.getKey();
            if(get(fork_stack_id) != null && !replace_existing)
                continue;

            ProtocolStack  fork_prot_stack=new ForkProtocolStack();
            List<Protocol> prots=createProtocols(fork_prot_stack,entry.getValue());
            createForkStack(fork_stack_id, fork_prot_stack, replace_existing, prots);
        }
    }

    public void parse(Node node) throws Exception {
        Map<String,List<ProtocolConfiguration>> protocols=ForkConfig.parse(node);
        createForkStacks(protocols, false);
    }

    /**
     * Creates a new fork-stack from protocols and adds it into the hashmap of fork-stack (key is fork_stack_id).
     * Method init() will be called on each protocol, from bottom to top.
     * @param fork_stack_id The key under which the new fork-stack should be added to the fork-stacks hashmap
     * @param stack The protocol stack under which to create the protocols
     * @param replace_existing If true, and existing fork-stack is simply replaced (de-initializing it first).
     *                         Otherwise, if the stack already exists, the new fork-stack will not be created.
     * @param protocols A list of protocols from <em>bottom to top</em> to be inserted. They will be sandwiched
     *                  between ForkProtocolStack (top) and ForkProtocol (bottom). The list can be empty (or null) in
     *                  which case we won't create any protocols, but still have a separate fork-stack inserted.
     * @return The bottom-most protocol of the new stack, or the existing stack (if present)
     */
    public Protocol createForkStack(String fork_stack_id, final ProtocolStack stack, boolean replace_existing,
                                    List<Protocol> protocols) throws Exception {
        Protocol bottom;
        if((bottom=get(fork_stack_id)) != null && !replace_existing) {
            log.warn("fork-stack %s is already present, won't replace it", fork_stack_id);
            return bottom;
        }

        Protocol current=stack;                          // top
        if(protocols != null && !protocols.isEmpty()) {
            for(int i=protocols.size()-1; i >= 0; i--) {
                Protocol prot=protocols.get(i);
                current.setDownProtocol(prot);
                prot.setUpProtocol(current);
                current=prot;
            }
        }

        bottom=new ForkProtocol(fork_stack_id); // bottom
        current.setDownProtocol(bottom);
        bottom.setUpProtocol(current);
        bottom.setDownProtocol(this);
        stack.topProtocol(stack.getDownProtocol()).bottomProtocol(bottom);
        fork_stacks.put(fork_stack_id, bottom);

        // call init() on the created protocols, from bottom to top
        //current=bottom;
        while(current != null && !(current instanceof ProtocolStack)) {
            current.init();
            current=current.getUpProtocol();
        }
        return bottom;
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
        catch(FileNotFoundException fnfe) {
        }
        catch(AccessControlException access_ex) { // fixes http://jira.jboss.com/jira/browse/JGRP-94
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

        public int size() {return Util.size(fork_stack_id) + Util.size(fork_channel_id);}

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
