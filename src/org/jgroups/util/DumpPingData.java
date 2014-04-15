package org.jgroups.util;

import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.PingData;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;

/**
 * Dumps the contents of {@link org.jgroups.protocols.PingData} stored in files to stdout. Used by file
 * based discovery protocols such as {@link org.jgroups.protocols.FILE_PING}.
 * @author Bela Ban
 * @since  3.5
 */
public class DumpPingData {
    protected String location;
    protected String cluster_name;

    public DumpPingData(String location, String cluster_name) {
        this.location=location;
        this.cluster_name=cluster_name;
    }

    protected void start() throws Exception {
        File root=new File(location);
        if(!root.exists())
            throw new FileNotFoundException("location " + location + " does not exist");
        if(cluster_name != null) {
            File tmp=new File(location + File.separator + cluster_name);
            if(!tmp.exists())
                throw new FileNotFoundException("Directory " + tmp + " does not exist");
            listFiles(tmp);
            return;
        }
        for(File dir: root.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isDirectory();
            }
        })) {
            listFiles(dir);
        }
    }

    protected static void listFiles(File dir) {
        for(File file: dir.listFiles(new FileFilter() {
            public boolean accept(File tmpfile) {
                return tmpfile.getAbsolutePath().endsWith("node");
            }
        })) {
            try {
                PingData data=FILE_PING.readFile(file);
                System.out.println(data);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String location=null;
        String cluster_name=null;
        String filename=null;

        for(int i=0; i < args.length; i++) {
            if("-location".equals(args[i])) {
                location=args[++i];
                continue;
            }
            if("-cluster".equals(args[i])) {
                cluster_name=args[++i];
                continue;
            }
            if("-file".equals(args[i])) {
                filename=args[++i];
                continue;
            }
            help();
            return;
        }

        if(filename != null) {
            try {
                PingData data=FILE_PING.readFile(new File(filename));
                System.out.println(data);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            return;
        }

        if(location == null) {
            System.err.println("Location cannot be null");
            return;
        }

        new DumpPingData(location, cluster_name).start();
    }



    protected static void help() {
        System.out.println("DumpPingData [-location <dir>] [-cluster <cluster name>] [-file <filename>]");
    }
}
