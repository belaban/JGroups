package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.GridInputStream;
import org.jgroups.blocks.GridFile;
import org.jgroups.blocks.GridFilesystem;
import org.jgroups.util.Util;

import java.io.FileOutputStream;
import java.io.File;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @version $Id: GridInputStreamTest.java,v 1.3 2009/12/28 13:15:33 belaban Exp $
 */
public class GridInputStreamTest {
    
    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="imfs-cluster";
        String metadata_cluster_name="metadata-cluster";
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
            if(args[i].equals("-metadata_cluster_name")) {
                metadata_cluster_name=args[++i];
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
            System.out.println("GridInputStreamTest [-props <JGroups config>] [-cluster_name <cluster name] " +
                    "[-metadata_cluster_name <name>] " +
                    "[-input_file <file to read from cluster>]" +
                    "[-output_file <path to file to write to file system>]");
            return;
        }


        ReplCache<String,byte[]> data=new ReplCache<String,byte[]>(props, cluster_name);
        ReplCache<String, GridFile.Metadata> metadata=new ReplCache<String, GridFile.Metadata>(props, metadata_cluster_name);
        data.start();
        metadata.start();

        GridFilesystem file_system=new GridFilesystem(data, metadata);

        // Create parent dir if it doesn't exist
        File file=file_system.getFile(new File(input_file).getParent());
        file.mkdirs();
        InputStream input=file_system.getInput(input_file);

        FileOutputStream out=output_file != null?  new FileOutputStream(output_file) : null;
        byte[] buf=new byte[50000];
        int total_bytes=0;
        int bytes_read;
        long start=System.currentTimeMillis();
        while((bytes_read=input.read(buf, 0, buf.length)) != -1) {
            if(out != null)
                out.write(buf, 0, bytes_read);
            total_bytes+=bytes_read;
        }
        long diff=System.currentTimeMillis() - start;

        Util.close(input);
        Util.close(out);

        data.stop();
        metadata.stop();

        double throughput=total_bytes / (diff / 1000.0);
        System.out.println("read " + Util.printBytes(total_bytes) + " bytes in " + diff + " ms, " +
                "throughput=" + Util.printBytes(throughput) + " / sec");
    }
}