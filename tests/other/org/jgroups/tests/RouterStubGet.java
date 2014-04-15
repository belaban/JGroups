package org.jgroups.tests;

import org.jgroups.protocols.PingData;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Responses;

/**
 * @author Bela Ban
 */
public class RouterStubGet {
    public static void main(String[] args) throws Exception {
        String host="localhost";
        int port=12001;
        String cluster_name="draw";

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
            help();
            return;
        }

        RouterStub stub=new RouterStub(host, port, null,null);
        stub.doConnect();
        Responses rsps=new Responses(false);
        stub.getMembers(cluster_name, rsps);
        for(PingData data: rsps)
            System.out.println(data);
        stub.destroy();
    }

    private static void help() {
        System.out.println("RouterStubGet [-host <host>] [-port <port>] [-cluster <cluster name (default: draw>]");
    }
}
