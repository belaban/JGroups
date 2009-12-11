package org.jgroups.blocks;

import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.util.Date;
import java.util.Set;

/**
 * Subclass of File to iterate through directories and files in a grid
 * @author Bela Ban
 * @version $Id: GridFile.java,v 1.1 2009/12/11 13:22:42 belaban Exp $
 */
public class GridFile extends File {
    private static final long serialVersionUID=-6729548421029004260L;
    private final ReplCache<String,Metadata> cache;
    private final String name;
    private final int chunk_size;

    public GridFile(String pathname, ReplCache<String, Metadata> cache, int chunk_size) {
        super(pathname);
        this.name=pathname;
        this.cache=cache;
        this.chunk_size=chunk_size;
    }

    public GridFile(String parent, String child, ReplCache<String, Metadata> cache, int chunk_size) {
        super(parent, child);
        this.name=parent + File.separator + child;
        this.cache=cache;
        this.chunk_size=chunk_size;
    }

    public GridFile(File parent, String child, ReplCache<String, Metadata> cache, int chunk_size) {
        super(parent, child);
        this.name=parent.getAbsolutePath() + File.separator + child;
        this.cache=cache;
        this.chunk_size=chunk_size;
    }

    public GridFile(URI uri, ReplCache<String, Metadata> cache, int chunk_size) {
        super(uri);
        this.name=getAbsolutePath();
        this.cache=cache;
        this.chunk_size=chunk_size;
    }

    public boolean createNewFile() throws IOException {
        if(exists())
            return false;
        if(!checkParentDirs(name, false))
            return false;
        cache.put(name, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.FILE), (short)-1, 0);
        return true;
    }

    public boolean mkdir() {
        try {
            boolean parents_exist=checkParentDirs(name, false);
            if(!parents_exist)
                return false;
            cache.put(name, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0);
            return true;
        }
        catch(IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean mkdirs() {
        try {
            boolean parents_exist=checkParentDirs(name, true);
            if(!parents_exist)
                return false;
            cache.put(name, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0);
            return true;
        }
        catch(IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean exists() {
        return cache.get(name) != null;
    }

    public String[] list() {
        Cache<String, ReplCache.Value<Metadata>> internal_cache=cache.getL2Cache();
        Set<String> keys=internal_cache.getInternalMap().keySet();
        if(keys == null)
            return null;
        String[] retval=new String[keys.size()];
        int index=0;
        for(String str: keys) {
            retval[index++]=str;
        }
        return retval;
    }

    /**
     * Checks whether the parent directories are present (and are directories). If create_if_absent is true,
     * creates missing dirs
     * @param path
     * @param create_if_absent
     * @return
     */
    private boolean checkParentDirs(String path, boolean create_if_absent) throws IOException {
        String[] components=components(path);
        if(components == null)
            return false;
        if(components.length == 1) // no parent directories to create, e.g. "data.txt"
            return true;

        StringBuilder sb=new StringBuilder(File.separator);
        boolean first=true;

        for(int i=0; i < components.length-1; i++) {
            String tmp=components[i];
            if(first)
                first=false;
            else
                sb.append(File.separator);
            sb.append(tmp);
            String comp=sb.toString();
            if(exists(comp)) {
                if(isFile(comp))
                    throw new IOException("cannot create " + path + " as component " + comp + " is a file");
            }
            else {
                cache.put(comp, new Metadata(0, System.currentTimeMillis(), chunk_size, Metadata.DIR), (short)-1, 0);
            }
        }
        return true;
    }

    private static String[] components(String path) {
        if(path == null)
            return null;
        path=path.trim();
        int index=path.indexOf(File.separator);
        if(index == 0)
            path=path.substring(1);
        return path.split(File.separator);
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
            return "length=" + length + " bytes, modification_time=" + new Date(modification_time) +
                    ", chunk_size=" + chunk_size + ", type=" + getType();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            out.writeInt(length);
            out.writeLong(modification_time);
            out.writeInt(chunk_size);
            out.writeByte(flags);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
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
