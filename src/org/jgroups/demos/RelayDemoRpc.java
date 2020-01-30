package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.*;


/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemoRpc implements Receiver {
    protected JChannel          ch;
    protected RpcDispatcher     disp;
    protected String            local_addr;
    protected View              view;

    protected static final long RPC_TIMEOUT=10000;



    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String name=null;


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            System.out.println("RelayDemo [-props props] [-name name]");
            return;
        }
        RelayDemoRpc demo=new RelayDemoRpc();
        demo.start(props, name);
    }

    public void receive(Message msg) {

    }

    public void start(String props, String name) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        disp=new RpcDispatcher(ch, this).setReceiver(this);
        ch.connect("RelayDemo");
        local_addr=ch.getAddress().toString();

        MethodCall call=new MethodCall(getClass().getMethod("handleMessage", String.class, String.class));
        for(;;) {
            String line=Util.readStringFromStdin(": ");
            if(line.startsWith("help")) {
                System.out.println("unicast <text>  // unicasts to all members of local view\n" +
                                     "site <site>+    // unicasts to all listed site masters, e.g. \"site sfo lon\"\n" +
                                     "mcast <site>+   // anycasts to all local members, plus listed site masters \n" +
                                     "<text>          // multicast, RELAY2 will relay to all members of sites");
                continue;
            }


            call.args(line, local_addr);

            // unicast to every member of the local cluster
            if(line.equalsIgnoreCase("unicast")) {
                for(Address dest: view.getMembers()) {
                    System.out.println("invoking method in " + dest + ": ");
                    try {
                        Object rsp=disp.callRemoteMethod(dest, call, new RequestOptions(ResponseMode.GET_ALL, RPC_TIMEOUT));
                        System.out.println("rsp from " + dest + ": " + rsp);
                    }
                    catch(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
            }

            // unicast to 1 SiteMaster
            else if(line.startsWith("site")) {
                Collection<String> site_masters=parseSiteMasters(line.substring("site".length()));
                for(String site_master: site_masters) {
                    try {
                        SiteMaster dest=new SiteMaster(site_master);
                        System.out.println("invoking method in " + dest + ": ");
                        Object rsp=disp.callRemoteMethod(dest, call, new RequestOptions(ResponseMode.GET_ALL, RPC_TIMEOUT));
                        System.out.println("rsp from " + dest + ": " + rsp);
                    }
                    catch(Throwable t) {
                        t.printStackTrace();
                    }
                }
            }

            // mcast to all local members and N SiteMasters
            else if(line.startsWith("mcast")) {
                Collection<String> site_masters=parseSiteMasters(line.substring("mcast".length()));
                Collection<Address> dests=new ArrayList<>(site_masters.size());
                for(String site_master: site_masters) {
                    try {
                        dests.add(new SiteMaster(site_master));
                    }
                    catch(Throwable t) {
                        System.err.println("failed adding SiteMaster for " + site_master + ": " + t);
                    }
                }
                dests.addAll(view.getMembers());
                System.out.println("invoking method in " + dests + ": ");
                RspList<Object> rsps=disp.callRemoteMethods(dests, call,
                                                            new RequestOptions(ResponseMode.GET_ALL, RPC_TIMEOUT).anycasting(true));
                for(Map.Entry<Address,Rsp<Object>> entry: rsps.entrySet()) {
                    Address sender=entry.getKey();
                    Rsp<Object> rsp=entry.getValue();
                    if(rsp.wasUnreachable())
                        System.out.println("<< unreachable: " + sender);
                    else
                        System.out.println("<< " + rsp.getValue() + " from " + sender);
                }
            }
            else {
                // mcasting the call to all local cluster members
                RspList<Object> rsps=disp.callRemoteMethods(null, call,
                                                            new RequestOptions(ResponseMode.GET_ALL, RPC_TIMEOUT).anycasting(false));
                rsps.forEach((key, val) -> System.out.println("<< " + val.getValue() + " from " + key));
            }
        }
    }

    protected static Collection<String> parseSiteMasters(String line) {
        Set<String> retval=new HashSet<>();
        String[] tmp=line.split("\\s");
        for(String s: tmp) {
            String result=s.trim();
            if(!result.isEmpty())
                retval.add(result);
        }
        return retval;
    }

    public static String handleMessage(String msg, String sender) {
        System.out.println("<< " + msg + " from " + sender);
        return "this is a response";
    }


    public void viewAccepted(View new_view) {
        System.out.println(print(new_view));
        view=new_view;
    }


    static String print(View view) {
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
