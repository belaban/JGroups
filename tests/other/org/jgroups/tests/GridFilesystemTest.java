package org.jgroups.tests;

import org.jgroups.blocks.GridFile;
import org.jgroups.blocks.GridFilesystem;
import org.jgroups.blocks.ReplCache;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: GridFilesystemTest.java,v 1.11 2009/12/30 14:43:25 belaban Exp $
 * todo: regexp for file ls, rm, down and up
 * todo: recursive up and down
 */
public class GridFilesystemTest {
    static final Map<String,Command> commands=new HashMap<String,Command>();
    static String current_dir="/";
    static final String HOME=System.getProperty("user.home");

    static {
        commands.put("mkdir", new mkdir());
        commands.put("ls",    new ls());
        commands.put("cd",    new cd());
        commands.put("pwd",   new pwd());
        commands.put("rm",    new rm());
        commands.put("up",    new up());
        commands.put("down",  new down());
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
                print(file, detailed, recursive, 0, file.isDirectory());
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
                if(!dir.startsWith(File.separator))
                    dir=current_dir.equals(File.separator)? current_dir + dir : current_dir + File.separator + dir;
                File file=fs.getFile(dir);
                if(!file.exists()) {
                    System.err.println(file.getName() + " doesn't exist");
                    return;
                }
                if(file.isFile()) {
                    if(!file.delete())
                        System.err.println("cannot remove " + file.getName());
                    return;
                }

                if(!recursive) {
                    if(!file.delete())
                        System.err.println("cannot remove " + file.getName() + ": is a directory");
                }
                else {
                    if(!delete(file)) { // recursive delete
                        System.err.println("recursive removal of " + file.getName() + " failed");
                    }
                }
            }
        }

        public String help() {
            return "rm [-fr] <files or dirs>";
        }
    }


    private static class cd implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String[] tmp=getNonOptions(args);
            String target_dir="~";
            if(tmp != null && tmp.length == 1)
                target_dir=tmp[0];

            if(target_dir.equals(".."))
                target_dir=new File(current_dir).getParent();
            if(target_dir.contains("~") && HOME != null)
                target_dir=target_dir.replace("~", HOME);
            if(!target_dir.trim().startsWith(File.separator)) {
                target_dir=current_dir.equals(File.separator)?
                        current_dir + target_dir :
                        current_dir + File.separator + target_dir;
            }
            File dir=fs.getFile(target_dir);
            if(!dir.exists()) {
                System.err.println("Directory " + target_dir + " doesn't exist");
            }
            else
                current_dir=target_dir;
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


    private static class up implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String   options=parseOptions(args);
            String[] real_args=getNonOptions(args);
            boolean  overwrite=options.contains("f");

            if(real_args.length == 0) {
                System.err.println(help());
                return;
            }

            String local_path=real_args[0], grid_path=real_args.length > 1? real_args[1] : null;
            if(HOME != null && local_path.contains("~"))
                local_path=local_path.replace("~", HOME);
            String target_path=grid_path != null? grid_path : local_path;
            if(target_path.contains("~"))
                target_path=target_path.replace("~", HOME);
            if(target_path.equals("."))
                target_path=current_dir;

            File target=fs.getFile(target_path);
            if(!overwrite && target.exists() && target.isFile()) {
                System.err.println("grid file " + target_path + " already exists; use -f to force overwrite");
                return;
            }
            
            if(target.exists() && target.isDirectory()) {
                String[] comps=Util.components(local_path, File.separator);
                String filename=comps[comps.length - 1];
                target_path=target_path + (target_path.equals(File.separator)? filename : File.separator + filename);
            }

            FileInputStream in=null;
            OutputStream out=null;
            try {
                in=new FileInputStream(local_path);
                File out_file=fs.getFile(target_path);
                if(out_file.exists() && out_file.isDirectory()) {
                    System.err.println("target " + target_path + " is a directory");
                    return;
                }
                if(out_file.exists() && out_file.isFile() && overwrite) {
                    if(out_file instanceof GridFile)
                        ((GridFile)out_file).delete(true);
                    else
                        out_file.delete();
                }

                out=fs.getOutput((GridFile)out_file);
                byte[] buf=new byte[50000];
                int len, total=0;
                while((len=in.read(buf, 0, buf.length)) != -1) {
                    out.write(buf, 0, len);
                    total+=len;
                }
                System.out.println("uploaded " + local_path + " to " + target_path + " (" + total + " bytes)");
            }
            catch(FileNotFoundException e) {
                System.err.println("local file " + local_path + " not found");
            }
            catch(IOException e) {
                System.err.println("cannot create " + target_path);
            }
            finally {
                Util.close(in);
                Util.close(out);
            }
        }

        public String help() {
            return "up [-f] <local path> [<grid path>]";
        }
    }


    private static class down implements Command {

        public void execute(GridFilesystem fs, String[] args) {
            String   options=parseOptions(args);
            String[] real_args=getNonOptions(args);
            boolean  overwrite=options.contains("f");

            if(real_args.length == 0) {
                System.err.println(help());
                return;
            }

            String grid_path=real_args[0], local_path=real_args.length > 1? real_args[1] : null;
            if(HOME != null && grid_path.contains("~"))
                grid_path=grid_path.replace("~", HOME);
            String target_path=local_path != null? local_path : grid_path;
            if(target_path.contains("~"))
                target_path=target_path.replace("~", HOME);
            if(target_path.equals("."))
                target_path=System.getProperty("user.dir");

            File target=new File(target_path);
            if(!overwrite && target.exists() && target.isFile()) {
                System.err.println("grid file " + target_path + " already exists; use -f to force overwrite");
                return;
            }

            if(target.exists() && target.isDirectory()) {
                String[] comps=Util.components(grid_path, File.separator);
                String filename=comps[comps.length - 1];
                target_path=target_path + (target_path.endsWith(File.separator)? filename : File.separator + filename);
            }

            InputStream in=null;
            FileOutputStream out=null;
            try {
                in=fs.getInput(grid_path);
                File out_file=new File(target_path);
                if(out_file.exists() && out_file.isDirectory()) {
                    System.err.println("target " + target_path + " is a directory");
                    return;
                }
                out=new FileOutputStream(out_file);
                byte[] buf=new byte[50000];
                int len, total=0;
                while((len=in.read(buf, 0, buf.length)) != -1) {
                    out.write(buf, 0, len);
                    total+=len;
                }
                System.out.println("downloaded " + local_path + " to " + target_path + " (" + total + " bytes)");
            }
            catch(FileNotFoundException e) {
                System.err.println("local file " + local_path + " not found");
            }
            catch(IOException e) {
                System.err.println("cannot create " + target_path);
            }
            finally {
                Util.close(in);
                Util.close(out);
            }
        }

        public String help() {
            return "down [-f] <grid path> [<local path>]";
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

    private static boolean delete(File dir) {
        boolean retval=true;
        if(dir == null)
            return false;
        File[] files=dir.listFiles();
        if(files != null) {
            for(File file: files) {
                if(!delete(file))
                    retval=false;
            }
        }
        boolean rc=dir instanceof GridFile? ((GridFile)dir).delete(true) : dir.delete();
        if(!rc)
            retval=false;
        return retval;
    }

    private static void print(File file, boolean details, boolean recursive, int indent, boolean exclude_self) {
        if(file.isDirectory()) {
            if(!exclude_self)
                System.out.print(print(file, details, indent));
            File[] children=file.listFiles();
            for(File child: children) {
                if(!recursive)
                    System.out.print(print(child, details, indent));
                else {
                    print(child, details, recursive, indent +4, false);
                }
            }
            if(children.length > 0)
                System.out.println("");
        }
        else {
            String tmp=print(file, details, indent);
            System.out.print(tmp);
        }
    }

    private static String print(File file, boolean details, int indent) {
        StringBuilder sb=new StringBuilder();
        if(file.isDirectory()) {
            if(details)
                sb.append(indent(indent));
            sb.append(file.getName()).append("/");
        }
        else {
            if(details)
                sb.append(indent(indent));
            sb.append(file.getName());
            if(details) {
                sb.append(" " + Util.printBytes(file.length()));
                if(file instanceof GridFile)
                    sb.append(", chunk_sise=" + ((GridFile)file).getChunkSize());
            }

        }
        sb.append(details? '\n' : ' ');
        return sb.toString();
    }

    private static String indent(int num) {
        StringBuilder sb=new StringBuilder();
        for(int i=0; i < num; i++)
            sb.append(' ');
        return sb.toString();
    }


}