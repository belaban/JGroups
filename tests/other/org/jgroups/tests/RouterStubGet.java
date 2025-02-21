package org.jgroups.tests;

import org.jgroups.protocols.PingData;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Promise;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * Class to fetch information about members of a given cluster (or members of all clusters)
 * @author Bela Ban
 */
public class RouterStubGet implements RouterStub.MembersNotification {
    protected RouterStub    stub;
    protected Promise<Void> promise=new Promise<>();
    protected int           cnt=1;

    protected void start(String host, int port, String cluster_name, boolean nio, long timeout) {
        try {
            InetSocketAddress local=new InetSocketAddress((InetAddress)null,0), remote=new InetSocketAddress(host,port);
            stub=new RouterStub(local, remote, nio, null, null);
            stub.connect();
            stub.getMembers(cluster_name, this);
            promise.getResult(timeout);
        }
        catch(Exception ex) {
            ex.printStackTrace();
            stub.destroy();
        }
    }

    @Override
    public void members(String group, List<PingData> mbrs, boolean last) {
        for(PingData data: mbrs) {
            if(group != null)
                System.out.printf("%d: %s: %s\n", cnt++, group, data);
            else
                System.out.printf("%d: %s\n", cnt++, data);
        }
        if(last) {
            if(stub != null)
                stub.destroy();
            promise.setResult(null);
        }
    }

    @Override
    public void members(List<PingData> mbrs) {
        members(null, mbrs, false);
    }


    public static void main(String[] args) {
        String  host="localhost", cluster_name=null;
        int     port=12001;
        long    timeout=500;
        boolean nio=true;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-host")) {
                host=args[++i];
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-cluster")) {
                cluster_name=args[++i];
                continue;
            }
            if(args[i].equals("-nio")) {
                nio=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-timeout".equals(args[i])) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            help();
            return;
        }

        RouterStubGet get=new RouterStubGet();
        get.start(host, port, cluster_name, nio, timeout);
    }


    private static void help() {
        System.out.println("RouterStubGet [-host <host>] [-port <port>] [-cluster <cluster name (default: draw>] " +
                             "[-nio true|false] [-timeout <msecs>]");
    }
}
