package org.jgroups.demos;

import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;


/** Demos RELAY. Create 2 *separate* clusters with RELAY as top protocol. Each RELAY has bridge_props="tcp.xml" (tcp.xml
 * needs to be present). Then start 2 instances in the first cluster and 2 instances in the second cluster. They should
 * find each other, and typing in a window should send the text to everyone, plus we should get 4 responses.
 * @author Bela Ban
 */
public class RelayDemoRpc extends ReceiverAdapter {
    protected JChannel ch;
    protected RpcDispatcher disp;
    protected Address local_addr;
    protected View view;



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

    public void start(String props, String name) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.setName(name);
        disp=new RpcDispatcher(ch, this);
        ch.connect("RelayDemo");
        local_addr=ch.getAddress();

        MethodCall call=new MethodCall(getClass().getMethod("handleMessage", String.class, Address.class));
        for(;;) {
            String line=Util.readStringFromStdin(": ");
            call.setArgs(line, local_addr);
            if(line.equalsIgnoreCase("unicast")) {

                for(Address dest: view.getMembers()) {
                    System.out.println("invoking method in " + dest + ": ");
                    try {
                        Object rsp=disp.callRemoteMethod(dest, call, new RequestOptions(ResponseMode.GET_ALL, 5000));
                        System.out.println("rsp from " + dest + ": " + rsp);
                    }
                    catch(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
                continue;
            }

            RspList<Object> rsps=disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 5000).setAnycasting(true));
            for(Rsp rsp: rsps.values())
                System.out.println("<< " + rsp.getValue() + " from " + rsp.getSender());
        }
    }

    public static String handleMessage(String msg, Address sender) {
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
