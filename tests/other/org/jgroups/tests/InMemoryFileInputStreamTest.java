package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.InMemoryFileOutputStream;
import org.jgroups.blocks.InMemoryFileInputStream;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author Bela Ban
 * @version $Id: InMemoryFileInputStreamTest.java,v 1.1 2009/12/04 14:10:06 belaban Exp $
 */
public class InMemoryFileInputStreamTest {
    
    public static void main(String[] args) throws Exception {
        String props="/home/bela/fast.xml";
        String cluster_name="imfs-cluster";
        String input_file="/home/bela/tmp2.txt";
        String output_file=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-cluster_name")) {
                cluster_name=args[++i];
                continue;
            }
            if(args[i].equals("-output_file")) {
                output_file=args[++i];
                continue;
            }
            if(args[i].equals("-input_file")) {
                input_file=args[++i];
                continue;
            }
            System.out.println("InMemoryFileInputStreamTest [-props <JGroups config>] [-cluster_name <cluster name] " +
                    "[-input_file <file to read from cluster>]" +
                    "[-output_file <path to file to write to file system>]");
            return;
        }


        ReplCache<String,byte[]> cache=new ReplCache<String,byte[]>(props, cluster_name);
        cache.start();
        InMemoryFileInputStream input=new InMemoryFileInputStream(input_file, cache, 8000);

        FileOutputStream out=output_file != null?  new FileOutputStream("/home/bela/TimeScheduler2.java") : null;
        byte[] buf=new byte[50000];
        int total_bytes=0;
        int bytes_read;
        while((bytes_read=input.read(buf, 0, buf.length)) != -1) {
            if(out != null)
                out.write(buf, 0, bytes_read);
            total_bytes+=bytes_read;
        }

        Util.close(input);
        Util.close(out);

        cache.stop();

        System.out.println("read " + total_bytes + " bytes");
    }
}