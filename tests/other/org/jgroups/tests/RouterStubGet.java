package org.jgroups.tests;

import org.jgroups.protocols.PingData;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Promise;

import java.net.InetAddress;
import java.util.List;

/**
 * @author Bela Ban
 */
public class RouterStubGet implements RouterStub.MembersNotification {
    protected RouterStub    stub;
    protected Promise<Void> promise=new Promise<>();

    protected void start(String host, int port, String cluster_name, boolean nio) {
        try {
            stub=new RouterStub(null, 0, InetAddress.getByName(host), port, nio, null);
            stub.connect();
            stub.getMembers(cluster_name, this);
            promise.getResult(5000);
        }
        catch(Exception ex) {
            ex.printStackTrace();
            stub.destroy();
        }
    }

    @Override
    public void members(List<PingData> mbrs) {
        int cnt=1;
        for(PingData data: mbrs)
            System.out.printf("%d: %s\n", cnt++, data);
        if(stub != null)
            stub.destroy();
        promise.setResult(null);
    }


    public static void main(String[] args) {
        String host="localhost";
        int port=12001;
        String cluster_name="draw";
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
            help();
            return;
        }

        RouterStubGet get=new RouterStubGet();
        get.start(host, port, cluster_name, nio);
    }


    private static void help() {
        System.out.println("RouterStubGet [-host <host>] [-port <port>] [-cluster <cluster name (default: draw>] " +
                             "[-nio true|false]");
    }
}
