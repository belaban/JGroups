package org.jgroups.demos;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Simple pub-sub program using a shared transport: every topic creates a new channel
 * @author bela
 * @since 3.0
 */
public class PubSub {
    final Map<String,JChannel> topics=new HashMap<>();


    void start(String props) throws Exception {
        System.out.println("\n========== PubSub instance started =========");
        System.out.println("Valid commands are:");
        System.out.println("subscribe <topic>");
        System.out.println("unsubscribe <topic>");
        System.out.println("exit");
        System.out.println("print (prints all topics)");
        System.out.println("<topic>: <message>\n\n");

        System.out.println("Example");
        System.out.println("subscribe one\nsubscribe two\none: hello world\n\n");
        for(;;) {
            System.out.print("> ");
            String line=Util.readLine(System.in).trim();
            if(line.startsWith("subscribe")) {
                final String topic=line.substring("subscribe".length()).trim();
                if(!topics.containsKey(topic)) {
                    // we need to make sure "singleton_name" is set in the transport
                    JChannel ch=createSharedChannel("pubsub", props);
                    ch.setReceiver(new ReceiverAdapter() {
                        public void receive(Message msg) {
                            System.out.println("[" + topic + "] << " + msg.getObject());
                        }
                    });
                    ch.connect(topic);
                    topics.put(topic, ch);
                    System.out.println("subscribed to topic \"" + topic + "\"; current subscriptions: " + topics.keySet());
                }
                continue;
            }
            if(line.startsWith("unsubscribe")) {
                final String topic=line.substring("unsubscribe".length()).trim();
                JChannel ch=topics.remove(topic);
                if(ch == null) {
                    System.err.println("Topic \"" + topic + "\" not found");
                    continue;
                }
                Util.close(ch);
                System.out.println("unsubscribed from topic \"" + topic + "\"; current subscriptions: " + topics.keySet());
                continue;
            }
            if(line.startsWith("exit"))
                break;
            if(line.startsWith("print")) {
                System.out.println("topics: " + topics.keySet());
                continue;
            }
            int index=line.indexOf(":");
            if(index == -1) {
                // post to all topics
                for(JChannel ch: topics.values()) {
                    Message msg=new Message(null, null, line);
                    ch.send(msg);
                }
                continue;
            }
            String topic=line.substring(0, index).trim();
            String message=line.substring(index).trim();
            JChannel ch=topics.get(topic);
            if(ch == null) {
                System.err.println("sending to topic \"" + topic + "\" failed as topic doesn't exist, subscribe first");
                continue;
            }
            Message msg=new Message(null, null, message);
            ch.send(msg);
        }
        for(JChannel ch: topics.values())
            Util.close(ch);
    }

    private static JChannel createSharedChannel(String singleton_name, String props) throws Exception {
        ProtocolStackConfigurator config=ConfiguratorFactory.getStackConfigurator(props);
        List<ProtocolConfiguration> protocols=config.getProtocolStack();
        ProtocolConfiguration transport=protocols.get(0);
        transport.getProperties().put(Global.SINGLETON_NAME, singleton_name);
        return new JChannel(config);
    }

    
    public static void main(final String[] args) throws Exception {
        String props=null;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            help();
            return;
        }

        new PubSub().start(props);
    }


    protected static void help() {
        System.out.println("PubSub [-props props]");
        System.out.println("Valid commands are:");
        System.out.println("subscribe <topic>");
        System.out.println("unsubscribe <topic>");
        System.out.println("exit");
        System.out.println("print (prints all topics)");
        System.out.println("<topic>: <message>\n\n");

        System.out.println("Example");
        System.out.println("subscribe one\nsubscribe two\none: hello world");
    }




}
