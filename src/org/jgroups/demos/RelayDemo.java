package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.protocols.relay.RELAY2;
import org.jgroups.protocols.relay.RouteStatusListener;
import org.jgroups.util.Util;

/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemo implements Receiver {
    protected static final String SITE_MASTERS="site-masters";

    protected JChannel ch;
    protected RELAY2   relay;


    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;
        boolean print_route_status=false, nohup=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-print_route_status")) {
                print_route_status=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-nohup")) {
                nohup=true;
                continue;
            }
            System.out.println("RelayDemo [-props props] [-name name] [-print_route_status false|true] [-nohup]");
            return;
        }
        RelayDemo demo=new RelayDemo();
        demo.start(props, name, print_route_status, nohup);
    }

    public void receive(Message msg) {
        Address sender=msg.getSrc();
        System.out.println("<< " + msg.getObject() + " from " + sender);
        Address dst=msg.getDest();
        if(dst == null) {
            Message rsp=new BytesMessage(msg.getSrc(), "response");
            try {
                ch.send(rsp);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void viewAccepted(View new_view) {
        System.out.println(print(new_view));
    }

    protected void start(String props, String name, boolean print_route_status, boolean nohup) throws Exception {
        ch=new JChannel(props).setReceiver(this);
        if(name != null)
            ch.setName(name);
        relay=ch.getProtocolStack().findProtocol(RELAY2.class);
        if(relay == null)
            throw new IllegalStateException(String.format("Protocol %s not found", RELAY2.class.getSimpleName()));
        if(print_route_status) {
            relay.setRouteStatusListener(new RouteStatusListener() {
                public void sitesUp(String... sites) {
                    System.out.printf("-- %s: site(s) %s came up\n",
                                      ch.getAddress(), String.join(", ", sites));
                }

                public void sitesDown(String... sites) {
                    System.out.printf("-- %s: site(s) %s went down\n",
                                      ch.getAddress(), String.join(", ", sites));
                }
            });
        }
        ch.connect("RelayDemo");
        if(!nohup) {
            eventLoop(ch);
            Util.close(ch);
        }
    }

    protected void eventLoop(JChannel ch) {
        for(;;) {
            try {
                String line=Util.readStringFromStdin(": ");
                if(process(line)) // see if we have a command, otherwise pass down
                    continue;
                ch.send(null, line);
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    protected boolean process(String line) {
        if(line == null || line.isEmpty())
            return true;
        if(line.equalsIgnoreCase(SITE_MASTERS) || line.equalsIgnoreCase("sm")) {
            System.out.printf("site masters in %s: %s\n", relay.site(), relay.siteMasters());
            return true;
        }
        if(line.equalsIgnoreCase("help")) {
            help();
            return true;
        }
        if(line.equalsIgnoreCase("mbrs")) {
            System.out.printf("%s: local members: %s\n", relay.getLocalAddress(), relay.members());
            return true;
        }
        if(line.equalsIgnoreCase("sites")) {
            System.out.printf("configured sites: %s\n", relay.getSites());
            return true;
        }
        if(line.equalsIgnoreCase("topo")) {
            System.out.printf("\n%s\n", printTopology());
            return true;
        }

        return false;
    }

    protected static void help() {
        System.out.println("\ncommands:" +
                             "\nhelp" +
                             "\nmbrs: prints the local members" +
                             "\nsite-masters (sm): prints the site masters of this site" +
                             "\nsites: prints the configured sites" +
                             "\ntopo: prints the topology (site masters and local members of all sites)\n");
    }

    protected String printTopology() {
        return relay.printTopology(true);
    }

    protected static String print(View view) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        sb.append(view.getClass().getSimpleName() + ": ").append(view.getViewId()).append(": ");
        for(Address mbr: view.getMembers()) {
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(mbr);
        }
        return sb.toString();
    }
}
