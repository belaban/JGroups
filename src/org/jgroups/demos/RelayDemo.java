package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.protocols.relay.*;
import org.jgroups.protocols.relay.Topology;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemo implements Receiver {
    protected static final String SITE_MASTERS="site-masters", SENDTO="sendto";
    protected JChannel            ch;
    protected RELAY               relay;


    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;
        boolean print_route_status=true, nohup=false, use_view_handler=true;

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
            if("-use_view_handler".equals(args[i])) {
                use_view_handler=Boolean.parseBoolean(args[++i]);
                continue;
            }
            System.out.println("RelayDemo [-props props] [-name name] [-print_route_status false|true] " +
                                 "[-nohup] [-use_view_handler [true|false]]");
            return;
        }
        RelayDemo demo=new RelayDemo();
        demo.start(props, name, print_route_status, nohup, use_view_handler);
    }

    public void receive(Message msg) {
        Address sender=msg.getSrc();
        System.out.println("<< " + msg.getObject() + " from " + sender);
        Address dst=msg.getDest();
        if(dst == null) {
            Message rsp=new ObjectMessage(msg.getSrc(), "response");
            try {
                ch.send(rsp);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void viewAccepted(View new_view) {
        System.out.printf("Local view: %s\n", new_view);
    }

    protected void start(String props, String name, boolean print_route_status,
                         boolean nohup, boolean use_view_handler) throws Exception {
        ExtendedUUID.setPrintFunction(UUID::printName);
        ch=new JChannel(props).setReceiver(this);
        if(name != null)
            ch.setName(name);
        relay=ch.getProtocolStack().findProtocol(RELAY.class);
        if(relay == null)
            throw new IllegalStateException(String.format("Protocol %s not found", RELAY.class.getSimpleName()));
        String site_name=relay.site();
        if(print_route_status)
            relay.setRouteStatusListener(new DefaultRouteStatusListener(() -> relay.addr()).verbose(true));
        if(use_view_handler)
            relay.topo().setViewHandler((s, v) -> {
                if(!s.equals(relay.getSite()))
                    System.out.printf("Global view: %s\n", v);
            });
        ch.connect(site_name);
        if(!nohup) {
            eventLoop(ch);
            Util.close(ch);
        }
    }

    protected void eventLoop(JChannel ch) {
        for(;;) {
            try {
                String line=Util.readStringFromStdin(": ");
                if(line == null)
                    break;
                if(process(line)) // see if we have a command, otherwise pass down
                    continue;
                ObjectMessage msg=new ObjectMessage(null, line);
                ch.send(msg);
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
            System.out.printf("%s: local members: %s\n", relay.getAddress(), relay.members());
            return true;
        }
        if(line.equalsIgnoreCase("sites")) {
            System.out.printf("configured sites: %s\n", relay.getSites());
            return true;
        }
        if(line.startsWith("topo") && relay instanceof RELAY3) {
            System.out.printf("\n%s\n", printTopo(line, "topo", true));
            return true;
        }
        if(line.startsWith("pt") && relay instanceof RELAY3) {
            System.out.printf("\n%s\n", printTopo(line, "pt", false));
            return true;
        }
        if(line.startsWith("routes")) {
            System.out.printf("%s\n", relay.printRoutes());
            return true;
        }
        if(line.startsWith(SENDTO)) {
            sendTo(line);
            return true;
        }
        return false;
    }

    // sendto dest msg times sleep
    protected void sendTo(String line) {
        String[] list=line.split("\\s"); // whitespace, ie. " "
        long sleep=Long.parseLong(list[list.length-1]);
        int times=Integer.parseInt(list[list.length-2]);
        String dest=list[1];
        List<String> tmp=new ArrayList<>(Arrays.asList(list).subList(2, list.length - 2));
        String msg=String.join(" ", tmp);
        String[] s=dest.split(":");
        String member_name=s[0], site_name=s[1];
        Map<String,View> cache=relay.topo().cache();
        View v=cache.get(site_name);
        if(v == null)
            throw new IllegalArgumentException(String.format("site %s not found in cache", site_name));
        Address target=null;
        for(Address addr: v) {
            SiteUUID sa=(SiteUUID)addr;
            if(member_name.equals(sa.getName())) {
                target=sa;
                break;
            }
        }
        if(target == null)
            throw new IllegalArgumentException(String.format("member %s not found in cache", member_name));
        try {
            for(int i=1; i <= times; i++) {
                ch.send(target, String.format("%s [#%d]", msg, i));
                Util.sleep(sleep);
            }
        }
        catch(Exception ex) {
            System.err.printf("failed sending msg to %s: %s\n", target, ex);
        }
    }

    protected String printTopo(String line, String command, boolean refresh) {
        String sub=line.substring(command.length()).trim();
        String site=null;
        if(sub != null && !sub.isEmpty()) {
            String[] tmp=sub.split(" ");
            site=tmp.length > 0 && !tmp[0].isEmpty()? tmp[0].trim() : null;
        }
        Topology topo=relay.topo();
        if(refresh) {
            topo.removeAll(site != null? List.of(site) : null).refresh(site);
            Util.sleep(100);
        }
        return topo.print(site);
    }

    protected static void help() {
        System.out.println("\ncommands:" +
                             "\nhelp" +
                             "\nmbrs: prints the local members" +
                             "\nsite-masters (sm): prints the site masters of this site" +
                             "\nsites: prints the configured sites" +
                             "\ntopo: prints the topology (site masters and local members of all sites)" +
                             "\npt: prints the cache (no refresh)" +
                             "\nroutes: prints all routes (if site master)" +
                             "\nsendto <dest> msg <number of times> <sleep (ms)> (dest example: \"N:net1\")\n");
    }


}
