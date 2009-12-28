package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.GridOutputStream;
import org.jgroups.blocks.GridFilesystem;
import org.jgroups.blocks.GridFile;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.File;

/**
 * @author Bela Ban
 * @version $Id: GridOutputStreamTest.java,v 1.3 2009/12/28 13:15:33 belaban Exp $
 */
public class GridOutputStreamTest {

    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="imfs-cluster";
        String metadata_cluster_name="metadata-cluster";
        String input_file="/home/bela/profile3.jps";
        short default_repl_count=1;
        int default_chunk_size=4000;

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
            if(args[i].equals("-input_file")) {
                input_file=args[++i];
                continue;
            }
            if(args[i].equals("-repl_count")) {
                default_repl_count=Short.parseShort(args[++i]);
                continue;
            }
            if(args[i].equals("-chunk_size")) {
                default_chunk_size=Integer.parseInt(args[++i]);
                continue;
            }

            System.out.println("GridOutputStreamTest [-props <JGroups config>] [-cluster_name <name>] " +
                    "[-metadata_cluster_name <name>] [-repl_count <count>] [-chunk_size <size (bytes)>]" +
                    "[-input_file <path to file to place into cluster>]");
            return;
        }

        ReplCache<String,byte[]> data=new ReplCache<String,byte[]>(props, cluster_name);
        ReplCache<String, GridFile.Metadata> metadata=new ReplCache<String, GridFile.Metadata>(props, metadata_cluster_name);
        data.start();
        metadata.start();

        GridFilesystem file_system=new GridFilesystem(data, metadata,  default_repl_count, default_chunk_size);

        // Create parent dir if it doesn't exist
        File file=file_system.getFile(new File(input_file).getParent());
        file.mkdirs();

        OutputStream out=file_system.getOutput(input_file, false, default_repl_count, default_chunk_size);

        FileInputStream input=new FileInputStream(input_file);
        byte[] buf=new byte[50000];
        int bytes_read, total_bytes_written=0;
        while((bytes_read=input.read(buf, 0, buf.length)) != -1) {
            out.write(buf, 0, bytes_read);
            total_bytes_written+=bytes_read;
        }

        Util.close(input);
        Util.close(out);
        data.stop();
        metadata.stop();

        System.out.println("Wrote " + Util.printBytes(total_bytes_written) + " bytes into the cluster");
    }
}
