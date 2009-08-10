package org.jgroups.tests;

import org.jgroups.stack.RouterStub;
import org.jgroups.protocols.PingData;

import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: RouterStubGet.java,v 1.1 2009/08/10 12:29:18 belaban Exp $
 */
public class RouterStubGet {
    public static void main(String[] args) throws Exception {
        String host="localhost";
        int port=12001;
        String cluster_name="DrawGroupDemo";

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

        RouterStub stub=new RouterStub(host, port, null);
        stub.doConnect();
        List<PingData> responses=stub.getMembers(cluster_name);
        for(PingData data: responses)
            System.out.println(data);
        stub.destroy();
    }

    private static void help() {
        System.out.println("RouterStubGet [-host <host>] [-port <port>] [-cluster <cluster name (default: DrawGroupDemo>]");
    }
}
