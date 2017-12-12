package org.jgroups.blocks;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.jgroups.annotations.Experimental;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

/**
 * Subclass of File to iterate through directories and files in a grid
 * @author Bela Ban
 */
@Experimental
public class GridFile extends File {
    private static final long serialVersionUID=-6729548421029004260L;
    private final ReplCache<String,Metadata> cache;
    private final GridFilesystem fs;
    private final String name;
    private int chunk_size;

    GridFile(String pathname, ReplCache<String, Metadata> cache, int chunk_size, GridFilesystem fs) {
        super(pathname);
        this.fs=fs;
        this.name=trim(pathname);
        this.cache=cache;
        this.chunk_size=chunk_size;
        initMetadata();
    }

    GridFile(String parent, String child, ReplCache<String, Metadata> cache, int chunk_size, GridFilesystem fs) {
        super(parent, child);
        this.fs=fs;
        this.name=trim(parent + File.separator + child);
        this.cache=cache;
        this.chunk_size=chunk_size;
        initMetadata();
    }

    GridFile(File parent, String child, ReplCache<String, Metadata> cache, int chunk_size, GridFilesystem fs) {
        super(parent, child);
        this.fs=fs;
        this.name=trim(parent.getAbsolutePath() + File.separator + child);
        this.cache=cache;
        this.chunk_size=chunk_size;
        initMetadata();
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        String my_path=super.getPath();
        if(my_path != null && my_path.endsWith(File.separator)) {
            int index=my_path.lastIndexOf(File.separator);
            if(index != -1)
                my_path=my_path.substring(0, index);
        }
        return my_path;
    }

    public long length() {
        Metadata metadata=cache.get(getPath());
        if(metadata != null)
            return metadata.length;
        return 0;
    }

    void setLength(int new_length) {
        Metadata metadata=cache.get(getPath());
        if(metadata != null) {
            metadata.length=new_length;
            metadata.setModificationTime(System.currentTimeMillis());
            cache.put(getPath(), metadata, (short)-1, 0, false);
        }
        else
            System.err.println("metadata for " + getPath() + " not found !");
    }

    public int getChunkSize() {
        return chunk_size;
    }

    public boolean createNewFile() throws IOException {
        if(exists())
            return true;
        if(!checkParentDirs(getPath(), false))
            return false;
        cache.put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.FILE), (short)-1, 0, true);
        return true;
    }

    public boolean delete() {
        return delete(false); // asynchronous delete by default
    }

    public boolean delete(boolean synchronous) {
        if(!exists())
            return false;
        if(isFile()) {
            fs.remove(getPath(), synchronous);    // removes all the chunks belonging to the file
            cache.remove(getPath(), synchronous); // removes the metadata information
            return true;
        }
        if(isDirectory()) {
            File[] files=listFiles();
            if(files != null && files.length > 0)
                return false;
            fs.remove(getPath(), synchronous);    // removes all the chunks belonging to the file
            cache.remove(getPath(), synchronous); // removes the metadata information
        }
        return true;
    }

    public boolean mkdir() {
        try {
            boolean parents_exist=checkParentDirs(getPath(), false);
            if(!parents_exist)
                return false;
            cache.put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0, true);
            return true;
        }
        catch(IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean mkdirs() {
        try {
            boolean parents_exist=checkParentDirs(getPath(), true);
            if(!parents_exist)
                return false;
            cache.put(getPath(), new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0, true);
            return true;
        }
        catch(IOException e) {
            return false;
        }
    }

    public boolean exists() {
        return cache.get(getPath()) != null;
    }

    public String[] list() {
        return list(null);
    }

    public String[] list(FilenameFilter filter) {
        return _list(filter);
    }

    public File[] listFiles() {
        return listFiles((FilenameFilter)null);
    }

    public File[] listFiles(FilenameFilter filter) {
        return _listFiles(filter);
    }

    public File[] listFiles(FileFilter filter) {
        return _listFiles(filter);
    }


    public boolean isDirectory() {
        Metadata val=cache.get(getPath());
        return val.isDirectory();
    }

    public boolean isFile() {
        Metadata val=cache.get(getPath());
        return val.isFile();
    }

    protected void initMetadata() {
        Metadata metadata=cache.get(getPath());
        if(metadata != null)
            this.chunk_size=metadata.getChunkSize();
    }


    protected File[] _listFiles(Object filter) {
        String[] files=_list(filter);
        File[] retval=new File[files.length];
        for(int i=0; i < files.length; i++)
            retval[i]=new GridFile(files[i], cache, chunk_size, fs);
        return retval;
    }


    protected String[] _list(Object filter) {
        Cache<String, ReplCache.Value<Metadata>> internal_cache=cache.getL2Cache();
        Set<String> keys=internal_cache.getInternalMap().keySet();
        if(keys == null)
            return null;
        Collection<String> list=new ArrayList<>(keys.size());
        for(String str: keys) {
            if(isChildOf(getPath(), str)) {
                if(filter instanceof FilenameFilter &&  !((FilenameFilter)filter).accept(new File(name), filename(str)))
                    continue;
                else if(filter instanceof FileFilter && !((FileFilter)filter).accept(new File(str)))
                    continue;
                list.add(str);
            }
        }
        String[] retval=new String[list.size()];
        int index=0;
        for(String tmp: list)
            retval[index++]=tmp;
        return retval;
    }

    /**
     * Verifies whether child is a child (dir or file) of parent
     * @param parent
     * @param child
     * @return True if child is a child, false otherwise
     */
    protected static boolean isChildOf(String parent, String child) {
        if(parent == null || child == null)
            return false;
        if(!child.startsWith(parent))
            return false;
        if(child.length() <= parent.length())
            return false;
        int from=parent.equals(File.separator)? parent.length() : parent.length() +1;
        //  if(from-1 > child.length())
            // return false;
        String[] comps=Util.components(child.substring(from), File.separator);
        return comps != null && comps.length <= 1;
    }

    protected static String filename(String full_path) {
        String[] comps=Util.components(full_path, File.separator);
        return comps != null? comps[comps.length -1] : null;
    }



    /**
     * Checks whether the parent directories are present (and are directories). If create_if_absent is true,
     * creates missing dirs
     * @param path
     * @param create_if_absent
     * @return
     */
    protected boolean checkParentDirs(String path, boolean create_if_absent) throws IOException {
        String[] components=Util.components(path, File.separator);
        if(components == null)
            return false;
        if(components.length == 1) // no parent directories to create, e.g. "data.txt"
            return true;

        StringBuilder sb=new StringBuilder();
        boolean first=true;

        for(int i=0; i < components.length-1; i++) {
            String tmp=components[i];
            if(!tmp.equals(File.separator)) {
                if(first)
                    first=false;
                else
                    sb.append(File.separator);
            }
            sb.append(tmp);
            String comp=sb.toString();
            if(exists(comp)) {
                if(isFile(comp))
                    throw new IOException("cannot create " + path + " as component " + comp + " is a file");
            }
            else {
                if(create_if_absent)
                    cache.put(comp, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0);
                else
                    return false;
            }
        }
        return true;
    }



    protected static String trim(String str) {
        if(str == null) return null;
        str=str.trim();
        if(str.equals(File.separator))
            return str;
        String[] comps=Util.components(str, File.separator);
        return comps != null && comps.length > 0? comps[comps.length-1] : null;
    }

    private boolean exists(String key) {
        return cache.get(key) != null;
    }

    private boolean isFile(String key) {
        Metadata val=cache.get(key);
        return val.isFile();
    }



    
    public static class Metadata implements Streamable {
        public static final byte FILE = 1 << 0;
        public static final byte DIR  = 1 << 1;

        private int  length =0;
        private long modification_time=0;
        private int  chunk_size=0;
        private byte flags=0;


        public Metadata() {
        }

        public Metadata(int length, long modification_time, int chunk_size, byte flags) {
            this.length=length;
            this.modification_time=modification_time;
            this.chunk_size=chunk_size;
            this.flags=flags;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length=length;
        }

        public long getModificationTime() {
            return modification_time;
        }

        public void setModificationTime(long modification_time) {
            this.modification_time=modification_time;
        }

        public int getChunkSize() {
            return chunk_size;
        }

        public boolean isFile() {
            return Util.isFlagSet(flags, FILE);
        }

        public boolean isDirectory() {
            return Util.isFlagSet(flags, DIR);
        }

        public String toString() {
            boolean is_file=Util.isFlagSet(flags, FILE);
            StringBuilder sb=new StringBuilder();
            sb.append(getType());
            if(is_file)
                sb.append(", len=" + Util.printBytes(length) + ", chunk_size=" + chunk_size);
            sb.append(", mod_time=" + new Date(modification_time));
            return sb.toString();
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            out.writeInt(length);
            out.writeLong(modification_time);
            out.writeInt(chunk_size);
            out.writeByte(flags);
        }

        @Override
        public void readFrom(DataInput in) throws IOException {
            length=in.readInt();
            modification_time=in.readLong();
            chunk_size=in.readInt();
            flags=in.readByte();
        }

        private String getType() {
            if(Util.isFlagSet(flags, FILE))
                return "file";
            if(Util.isFlagSet(flags, DIR))
                return "dir";
            return "n/a";
        }
    }
}
