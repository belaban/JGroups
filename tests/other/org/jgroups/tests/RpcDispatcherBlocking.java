
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;




/**
 * Tests synchronous group RPCs. 2 main test cases:
 * <ol>
 * <li>Member crashes during invocation of sync group RPC: start 3 instances (A,
 * B,C), set the timeout to be 30 seconds. Then invoke a sync group RPC by A
 * (press 's' in A's window). A,B and C should receive the RPC. Now kill C.
 * After some time, A's method call should return and show A's and B's reply to
 * be valid, while showing C's response marked as suspected.
 * <li>Member joins group during synchronous group RPC: start A and B with
 * timeout=30000. Invoke a sync group RPC on A. Start C. A and B should
 * <em>not</em> receive the view change <em>before</em> the group RPC has
 * returned with A's and B's results. Therefore A and B should <em>not</em> wait
 * for C's response, which would never be received because C never got the RPC
 * in the first place. This would block A forever.
 * </ol>
 * 
 * @author bela Dec 19, 2002
 */
public class RpcDispatcherBlocking implements Receiver {
    RpcDispatcher disp;
    JChannel channel;
    long          timeout=30000;
    String        props=null;
    int           i=0;


    public RpcDispatcherBlocking(String props, long timeout) {
        this.props=props; this.timeout=timeout;
    }


    public void print(int i) throws Exception {
        System.out.println("<-- " + i + " [sleeping for " + timeout + " msecs");
        Util.sleep(timeout);
    }


    public void viewAccepted(View new_view) {
        System.out.println("new view: " + new_view);
    }


    public void start() throws Exception {
        int     c;
        RspList rsps;

        channel=new JChannel(); // default props
        disp=new RpcDispatcher(channel, this).setReceiver(this);
        channel.connect("rpc-test");
        
        while(true) {
            System.out.println("[x]: exit [s]: send sync group RPC");
            System.out.flush();
            c=System.in.read();
            switch(c) {
                case 'x':
                    channel.close();
                    disp.stop();
                return;
            case 's':
                rsps=sendGroupRpc();
                System.out.println("responses:\n" + rsps);
                break;
            }
            
            System.in.skip(System.in.available());
        }
    }


    RspList sendGroupRpc() throws Exception {
        return disp.callRemoteMethods(null, "print", new Object[]{i++}, new Class[] {int.class},
                                      new RequestOptions(ResponseMode.GET_ALL, 0));
    }


    public static void main(String[] args) {
        long   timeout=30000;
        String props=null;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-timeout".equals(args[i])) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            help();
            return;
        }
        


        try {
            new RpcDispatcherBlocking(props, timeout).start();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    static void help() {
        System.out.println("RpcDispatcherBlocking [-help] [-props <properties>] [-timeout <timeout>]");
    }
}
