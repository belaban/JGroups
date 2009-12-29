package org.jgroups.tests;

import org.jgroups.blocks.ReplCache;
import org.jgroups.blocks.GridOutputStream;
import org.jgroups.blocks.GridFilesystem;
import org.jgroups.blocks.GridFile;
import org.jgroups.util.Util;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

/**
 * @author Bela Ban
 * @version $Id: GridFilesystemTest.java,v 1.2 2009/12/29 15:26:20 belaban Exp $
 */
public class GridFilesystemTest {
    static final Map<String,Command> commands=new HashMap<String,Command>();
    static String current_dir="/";

    static {
        commands.put("mkdir", new mkdir());
        commands.put("ls",    new ls());
        commands.put("cd",    new cd());
        commands.put("pwd",   new pwd());
        commands.put("rm",    new rm());
    }

    public static void main(String[] args) throws Exception {
        String props="udp.xml";
        String cluster_name="imfs-cluster";
        String metadata_cluster_name="metadata-cluster";
        short default_repl_count=1;
        int default_chunk_size=4000;
        GridFilesystem fs;

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
            if(args[i].equals("-repl_count")) {
                default_repl_count=Short.parseShort(args[++i]);
                continue;
            }
            if(args[i].equals("-chunk_size")) {
                default_chunk_size=Integer.parseInt(args[++i]);
                continue;
            }

            System.out.println("GridFilesystemTest [-props <JGroups config>] [-cluster_name <name>] " +
                    "[-metadata_cluster_name <name>] [-repl_count <count>] [-chunk_size <size (bytes)>]");
            return;
        }

        ReplCache<String,byte[]> data=new ReplCache<String,byte[]>(props, cluster_name);
        ReplCache<String, GridFile.Metadata> metadata=new ReplCache<String, GridFile.Metadata>(props, metadata_cluster_name);
        data.start();
        metadata.start();

        fs=new GridFilesystem(data, metadata, default_repl_count, default_chunk_size);
        loop(data, metadata, fs);

        data.stop();
        metadata.stop();
    }

    private static void loop(ReplCache<String, byte[]> data, ReplCache<String, GridFile.Metadata> metadata, GridFilesystem fs) {
        while(true) {
            try {
                System.out.print("> ");
                String line=Util.readLine(System.in);
                if(!execute(fs, line))
                    break;
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static boolean execute(GridFilesystem fs, String line) {
        String[] comps=parseCommandLine(line);
        if(comps == null || comps.length == 0) {
            help();
            return true;
        }
        if(comps[0].equalsIgnoreCase("quit") || comps[0].equalsIgnoreCase("exit"))
            return false;
        if(comps[0].equalsIgnoreCase("help")) {
            help();
            return true;
        }
        Command cmd=commands.get(comps[0]);
        if(cmd == null) {
            System.err.println(comps[0] + " not known");
            help();
            return true;
        }
        String[] args=null;
        if(comps.length > 1) {
            args=new String[comps.length -1];
            System.arraycopy(comps, 1, args, 0, args.length);
        }
        cmd.execute(fs, args);
        return true;
    }

    private static void help() {
        StringBuilder sb=new StringBuilder("Valid commands:\nhelp\nquit\nexit\n");
        for(Command cmd: commands.values()) {
            String help=cmd.help();
            if(help != null)
                sb.append(help).append("\n");
        }
        System.out.println(sb.toString());
    }

    private static String[] parseCommandLine(String line) {
        if(line == null)
            return null;
        return line.trim().split(" ");
    }


    private static interface Command {
        void execute(GridFilesystem fs, String[] args);
        String help();
    }

    private static class ls implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String options=parseOptions(args);
            boolean recursive=options.contains("R");
            boolean detailed=options.contains("l");
            String[] files=getNonOptions(args);
            if(files == null || files.length == 0)
                files=new String[]{current_dir};
            for(String str: files) {
                if(!str.startsWith(File.separator))
                    str=current_dir + File.separator + str;
                File file=fs.getFile(str);
                if(!file.exists()) {
                    System.err.println("File " + file + " doesn't exist");
                    continue;
                }
                if(file.isDirectory()) {
                    File[] children=file.listFiles();
                    for(File child: children) {
                        System.out.print(child.getName() + " ");
                        System.out.println("");
                    }
                    System.out.println("");
                }
                else if(file.isFile())
                    System.out.println(file.getPath());
            }
        }

        public String help() {
            return "ls [-lR] [dirs | files]";
        }
    }

    private static class mkdir implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String options=parseOptions(args);
            boolean recursive=options.contains("p");
            String[] dir_names=getNonOptions(args);
            if(dir_names == null || dir_names.length < 1) {
                System.err.println(help());
                return;
            }
            for(String dir: dir_names) {
                if(!dir.startsWith(File.separator))
                    dir=current_dir + File.separator + dir;
                File file=fs.getFile(dir);
                boolean result;
                if(recursive)
                    result=file.mkdirs();
                else
                    result=file.mkdir();
                if(!result)
                    System.err.println("failed creating " + dir);
            }
        }

        public String help() {
            return "mkdir [-p] dirs";
        }
    }


    private static class rm implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String options=parseOptions(args);
            boolean recursive=options.contains("r");
            String[] dir_names=getNonOptions(args);
            if(dir_names == null || dir_names.length < 1) {
                System.err.println(help());
                return;
            }
            for(String dir: dir_names) {
                File file=fs.getFile(dir);
                file.delete();
            }
        }

        public String help() {
            return "rm [-fr] <files or dirs>";
        }
    }


    private static class cd implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String[] tmp=getNonOptions(args);
            if(tmp != null && tmp.length == 1) {
                String target_dir=tmp[0];
                if(target_dir.equals(".."))
                    target_dir=new File(current_dir).getParent();
                if(!target_dir.trim().startsWith(File.separator))
                    target_dir=current_dir + File.separator + target_dir;
                File dir=fs.getFile(target_dir);
                if(!dir.exists()) {
                    System.err.println("Directory " + target_dir + " doesn't exist");
                }
                current_dir=target_dir;
            }
        }

        public String help() {
            return "cd [dir]";
        }
    }

    private static class pwd implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            System.out.println(current_dir);
        }

        public String help() {
            return "pwd";
        }
    }

    private static String parseOptions(String[] args) {
        StringBuilder sb=new StringBuilder();
        if(args == null)
            return "";
        for(String str: args) {
            if(str.startsWith("-"))
                sb.append(str.substring(1));
        }
        return sb.toString();
    }

    private static String[] getNonOptions(String[] args) {
        if(args == null)
            return null;
        int cnt=0;
        for(String str: args)
            if(!str.startsWith("-"))
                cnt++;
        String[] retval=new String[cnt];
        cnt=0;
        for(String str: args)
            if(!str.startsWith("-"))
                retval[cnt++]=str;
        return retval;
    }


}