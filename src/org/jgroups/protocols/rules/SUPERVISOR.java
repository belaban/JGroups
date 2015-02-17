package org.jgroups.protocols.rules;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Protocol which supervises other protocols. Essentially a rule engine, where rule conditions are checked periodically,
 * triggering (optional) actions. Example: start failure detection in FD is it isn't running<p/>
 * https://issues.jboss.org/browse/JGRP-1557
 * @author Bela Ban
 * @since  3.3
 */
@MBean(description="Supervises the running stack, taking corrective actions if necessary")
public class SUPERVISOR extends Protocol {
    protected Address                     local_addr;

    protected volatile View               view;

    // The timer used to run the rules on
    protected TimeScheduler               timer;

    // The last 50 executions, can be retrieved with executions()
    protected final BoundedList<String>   executions=new BoundedList<>(50);

    @Property(description="Location of an XML file listing the rules to be installed")
    protected String                      config;

    // hashmap of rules, keys are rule names and values futures to the rules
    protected final Map<String,Tuple<Rule,Future<?>>> rules=new HashMap<>();

    protected final List<EventHandler>    event_handlers=new ArrayList<>();

    @ManagedAttribute(description="The number of registered EventHandler")
    protected volatile int                num_event_handlers;


    protected static final String         RULES    = "rules";
    protected static final String         RULE     = "rule";
    protected static final String         NAME     = "name";
    protected static final String         CLASS    = "class";
    protected static final String         INTERVAL = "interval";



    public Address getLocalAddress()      {return local_addr;}
    public View    getView()              {return view;}

    @ManagedOperation(description="Prints the last N conditions that triggered a rule action")
    public String executions() {
        StringBuilder sb=new StringBuilder();
        for(String execution: executions)
            sb.append(execution + "\n");
        return sb.toString();
    }

    public void addCondition(String cond) {
        executions.add(new Date() + ": " + cond);
    }

    @ManagedAttribute(description="The number of rules currently installed")
    public int getNumRules() {
        return rules.size();
    }

    @ManagedOperation(description="Prints all currently installed rules")
    public String dumpRules() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<String,Tuple<Rule,Future<?>>> entry: rules.entrySet()) {
            String key=entry.getKey();
            Tuple<Rule,Future<?>> tuple=entry.getValue();
            Rule rule=tuple.getVal1();
            sb.append(key + ": " + rule.description() + "\n");
        }
        return sb.toString();
    }

    public void register(EventHandler handler) {
        if(handler != null) {
            event_handlers.add(handler);
            num_event_handlers=event_handlers.size();
        }
    }

    public void unregister(EventHandler handler) {
        if(handler != null) {
            event_handlers.remove(handler);
            num_event_handlers=event_handlers.size();
        }
    }

    public void init() throws Exception {
        super.init();
        timer=getTransport().getTimer();
        if(timer == null)
            throw new IllegalStateException("timer not found");
        if(config != null)
            parseConfig(config);
    }

    public void destroy() {
        for(Tuple<Rule,Future<?>> tuple: rules.values())
            tuple.getVal2().cancel(true);
        rules.clear();
        super.destroy();
    }

    /**
     * Installs a new rule
     * @param interval Number of ms between executions of the rule
     * @param rule The rule
     */
    public void installRule(long interval, Rule rule) {
        installRule(null, interval, rule);
    }

    /**
     * Installs a new rule
     * @param name The name of the rule
     * @param interval Number of ms between executions of the rule
     * @param rule The rule
     */
    public void installRule(String name, long interval, Rule rule) {
        rule.supervisor(this).log(log).init();
        Future<?> future=timer.scheduleAtFixedRate(rule, interval, interval, TimeUnit.MILLISECONDS);
        Tuple<Rule,Future<?>> existing=rules.put(name != null? name : rule.name(), new Tuple<Rule,Future<?>>(rule, future));
        if(existing != null)
            existing.getVal2().cancel(true);
    }

    @ManagedOperation(description="Installs the given rule with the given classname")
    public void installRule(String name, long interval, String classname) throws Exception {
        Class<Rule> clazz=Util.loadClass(classname,getClass());
        Rule rule=clazz.newInstance();
        installRule(name, interval, rule);
    }

    @ManagedOperation(description="Installs the given rule with the given classname")
    public void installRule(long interval, String classname) throws Exception {
        installRule(null, interval, classname);
    }

    @ManagedOperation(description="Uninstalls the named rule")
    public void uninstallRule(String name) {
        if(name != null) {
            Tuple<Rule,Future<?>> tuple=rules.remove(name);
            if(tuple != null) {
                tuple.getVal2().cancel(true);
                tuple.getVal1().destroy();
            }
        }
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }

        if(num_event_handlers > 0) {
            for(EventHandler handler: event_handlers) {
                try {
                    handler.down(evt);
                }
                catch(Throwable t) {
                    log.error("event handler failed handling down event", t);
                }
            }
        }

        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                handleView((View)evt.getArg());
                break;
        }
        if(num_event_handlers > 0) {
            for(EventHandler handler: event_handlers) {
                try {
                    handler.up(evt);
                }
                catch(Throwable t) {
                    log.error("event handler failed handling up event", t);
                }
            }
        }
        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        if(num_event_handlers > 0) {
            for(Message msg: batch) {
                for(EventHandler handler: event_handlers) {
                    try {
                        handler.up(new Event(Event.MSG, msg));
                    }
                    catch(Throwable t) {
                        log.error("event handler failed handling up event", t);
                    }
                }
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    protected void handleView(View view) {
        this.view=view;
    }

    protected void parseConfig(String filename) throws Exception {
        InputStream input=null;
        try {
            input=ConfiguratorFactory.getConfigStream(filename);
            parseRules(input);
        }
        finally {
            Util.close(input);
        }
    }

    protected void parseRules(InputStream input) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false); // for now
        DocumentBuilder builder=factory.newDocumentBuilder();
        Document document=builder.parse(input);
        Element root=document.getDocumentElement();
        match(RULES, root.getNodeName(), true);
        NodeList children=root.getChildNodes();
        if(children == null || children.getLength() == 0)
            return;
        for(int i=0; i < children.getLength(); i++) {
            Node node=children.item(i);
            if(node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            String element_name=node.getNodeName();
            if(RULE.equals(element_name))
                parseRule(node);
            else
                throw new Exception("expected <" + RULE + ">, but got " + "<" + element_name + ">");
        }
    }

    protected void parseRule(Node root) throws Exception {
        if(root.getNodeType() != Node.ELEMENT_NODE)
            return;
        NamedNodeMap attrs=root.getAttributes();
        if(attrs == null || attrs.getLength() == 0)
            return;
        Attr name_attr=(Attr)attrs.getNamedItem(NAME),
          classname_attr=(Attr)attrs.getNamedItem(CLASS),
          interval_attr=(Attr)attrs.getNamedItem(INTERVAL);

        Class<Rule> clazz=Util.loadClass(classname_attr.getValue(), getClass());
        Rule rule=clazz.newInstance();
        long interval=Long.parseLong(interval_attr.getValue());
        installRule(name_attr.getValue(), interval, rule);
    }

    protected static void match(String expected_name, String name, boolean is_element) throws Exception {
        if(!expected_name.equals(name))
            throw new Exception((is_element? "Element " : "Attribute ") + "\"" + name + "\" didn't match \"" + expected_name + "\"");
    }



}
