package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.GridOutputStream;
import org.jgroups.util.Util;

import java.io.FileInputStream;

/**
 * @author Bela Ban
 * @version $Id: GridOutputStreamTest.java,v 1.2 2009/12/04 16:27:59 belaban Exp $
 */
public class GridOutputStreamTest {

    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="imfs-cluster";
        String input_file="/home/bela/profile3.jps";

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-cluster_name")) {
                cluster_name=args[++i];
                continue;
            }
            if(args[i].equals("-input_file")) {
                input_file=args[++i];
                continue;
            }
            System.out.println("GridOutputStreamTest [-props <JGroups config>] [-cluster_name <cluster name] " +
                    "[-input_file <path to file to place into cluster>]");
            return;
        }

        ReplCache<String,byte[]> cache=new ReplCache<String,byte[]>(props, cluster_name);
        cache.start();
        GridOutputStream out=new GridOutputStream(input_file, cache, (short)1, 8000);

        FileInputStream input=new FileInputStream(input_file);
        byte[] buf=new byte[50000];
        int bytes_read, total_bytes_written=0;
        while((bytes_read=input.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, bytes_read);
            total_bytes_written+=bytes_read;
        }

        Util.close(input);
        Util.close(out);
        cache.stop();

        System.out.println("Wrote " + Util.printBytes(total_bytes_written) + " bytes into the cluster");
    }
}
