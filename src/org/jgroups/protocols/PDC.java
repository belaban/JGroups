package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.util.*;
import org.jgroups.util.UUID;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Persistent Discovery Cache. Caches mapping between logical and physical addresses on disk, merges them with the
 * results of the get physical address(es) events.
 * This is done by intercepting the get and set physical address(es) event. Needs to be placed between the transport and
 * the discovery protocol. The disk cache stores each mapping in a separate file, named by the logical address.
 *
 * @author Bela Ban
 * @since  3.3
 */
@MBean(description="Persistent Discovery Cache. Caches discovery information on disk.")
public class PDC extends Protocol {
    protected final ConcurrentMap<Address,PhysicalAddress> cache=new ConcurrentHashMap<>();

    /* -----------------------------------------    Properties     ----------------------------------------------- */
    @Property(description="The absolute path of the directory for the disk cache. The mappings will be stored as " +
      "individual files in this directory")
    protected String              cache_dir=File.separator + "tmp" + File.separator + "jgroups";



    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected static final String SUFFIX=".node";
    protected File                root_dir;
    protected FilenameFilter      filter;
    protected Address             local_addr;





    @ManagedOperation(description="Prints the contents of the address-physical address mappings")
    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,PhysicalAddress> entry: cache.entrySet()) {
            sb.append(entry.getKey() + ": " + entry.getValue() + "\n");
        }
        return sb.toString();
    }


    public void init() throws Exception {
        super.init();
        createDiskCacheFile();
        readCacheFromDisk(); // populates the cache from disk (if the file is present found)
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.GET_PHYSICAL_ADDRESS:
                Object addr=down_prot.down(evt);
                return addr != null? addr : cache.get(evt.getArg());

            case Event.GET_PHYSICAL_ADDRESSES:
                Collection<PhysicalAddress> addrs=(Collection<PhysicalAddress>)down_prot.down(evt);
                Collection<PhysicalAddress> tmp=new HashSet<>(addrs);
                tmp.addAll(cache.values());
                return tmp;

            case Event.GET_LOGICAL_PHYSICAL_MAPPINGS:
                Map<Address,PhysicalAddress> map=(Map<Address, PhysicalAddress>)down_prot.down(evt);
                Map<Address,PhysicalAddress> new_map=new HashMap<>(map);
                new_map.putAll(cache);
                return new_map;

            case Event.ADD_PHYSICAL_ADDRESS:
                Tuple<Address,PhysicalAddress> new_val=(Tuple<Address, PhysicalAddress>)evt.getArg();
                if(new_val != null) {
                    cache.put(new_val.getVal1(), new_val.getVal2());
                    writeNodeToDisk(new_val.getVal1(), new_val.getVal2());
                }
                break;
            case Event.REMOVE_ADDRESS:
                Address tmp_addr=(Address)evt.getArg();
                if(cache.remove(tmp_addr) != null)
                    removeNodeFromDisk(tmp_addr);
                break;
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
            case Event.VIEW_CHANGE:
                List<Address> members=((View)evt.getArg()).getMembers();
                cache.keySet().stream().filter(mbr -> !members.contains(mbr)).forEach(mbr -> {
                    cache.remove(mbr);
                    removeNodeFromDisk(mbr);
                });
                break;
        }
        return down_prot.down(evt);
    }



    protected void createDiskCacheFile() throws IOException {
        root_dir=new File(this.cache_dir);
        if(root_dir.exists()) {
            if(!root_dir.isDirectory())
                throw new IllegalArgumentException("location " + root_dir.getPath() + " is not a directory");
        }
        else {
            root_dir.mkdirs();
        }
        if(!root_dir.exists())
            throw new IllegalArgumentException("location " + root_dir.getPath() + " could not be accessed");

        filter=(dir, name1) -> name1.endsWith(SUFFIX);
    }

    /** Reads all mappings from disk */
    protected synchronized void readCacheFromDisk() {
        if(log.isDebugEnabled())
            log.debug("reading all mappings from disk cache " + root_dir);
        File[] files=root_dir.listFiles(filter);
        if(files == null)
            return;

        for(File file: files) {
            // implementing a simple spin lock doing a few attempts to read the file
            // this is done since the file may be written in concurrency and may therefore not be readable
            Mapping data=null;
            for(int i=0; i < 3; i++) {
                data=null;
                if(file.exists())
                    data=readAddressMapping(file);
                if(data != null)
                    break;
                else
                    Util.sleep(100);
            }

            if(data == null) {
                log.warn("failed parsing content in " + file.getAbsolutePath() + ": removing it ");
                deleteFile(file);
            }
            else {
                if(data != null && data.getLogicalAddr() != null && data.getPhysicalAddr() != null) {
                    cache.put(data.getLogicalAddr(), (PhysicalAddress)data.getPhysicalAddr());
                    if(data.getLogicalName() != null && NameCache.get(data.getLogicalAddr()) == null)
                        NameCache.add(data.getLogicalAddr(), data.getLogicalName());
                }
            }
        }
    }

    private synchronized Mapping readAddressMapping(File file) {
        DataInputStream in=null;
        try {
            in=new DataInputStream(new FileInputStream(file));
            Mapping mapping=new Mapping();
            mapping.readFrom(in);
            return mapping;
        }
        catch(Exception e) {
            log.debug("failed to read file : "+file.getAbsolutePath(), e);
            return null;
        }
        finally {
            Util.close(in);
        }
    }


    protected synchronized void writeNodeToDisk(Address logical_addr, PhysicalAddress physical_addr) {
        String filename=addressAsString(logical_addr);

        // first write all data to a temporary file
        // this is because the writing can be very slow under some circumstances
        File tmpFile=null, destination=null;
        try {
            tmpFile=writeToTempFile(root_dir, logical_addr, physical_addr, NameCache.get(logical_addr));
            if(tmpFile == null)
                return;

            destination=new File(root_dir, filename + SUFFIX);

            //do a file move, this is much faster and could be considered atomic on most operating systems
            FileChannel src_ch=new FileInputStream(tmpFile).getChannel();
            FileChannel dest_ch=new FileOutputStream(destination).getChannel();
            src_ch.transferTo(0,src_ch.size(),dest_ch);
            src_ch.close();
            dest_ch.close();
            if(log.isTraceEnabled())
                log.trace("Moved: " + tmpFile.getName() + "->" + destination.getName());
        }
        catch(Exception ioe) {
            log.error(Util.getMessage("AttemptToMoveFailedAt") + tmpFile.getName() + "->" + destination.getName(), ioe);
        }
        finally {
            deleteFile(tmpFile);
        }
    }

    /**
     * Writes the data to a temporary file.<br>
     * The file is stored in the same directory as the other cluster files but is given the <tt>.tmp</tmp> suffix
     * @param dir The disk cache root dir
     * @param logical_addr The logical address
     * @param physical_addr The physical address
     * @return
     */
    protected File writeToTempFile(File dir, Address logical_addr, Address physical_addr, String logical_name) throws Exception {
        DataOutputStream out=null;

        File file=null;
        String filename=null;
        try {
            file=File.createTempFile("temp", null, dir);
            filename=file.getName();
            out=new DataOutputStream(new FileOutputStream(file));
            Util.writeAddress(logical_addr, out);
            Util.writeAddress(physical_addr, out);
            Bits.writeString(logical_name,out);
            Util.close(out);
            if(log.isTraceEnabled())
                log.trace("Stored temporary file: " + file.getAbsolutePath());
        }
        catch(Exception e) {
            Util.close(out);
            log.error(Util.getMessage("FailedToWriteTemporaryFile") + filename, e);
            deleteFile(file);
            return null;
        }
        return file;
    }


    protected synchronized void removeNodeFromDisk(Address logical_addr) {
        String filename=addressAsString(logical_addr);
        deleteFile(new File(root_dir, filename + SUFFIX));
    }


    protected static String addressAsString(Address address) {
        if(address == null)
            return "";
        if(address instanceof UUID)
            return ((UUID) address).toStringLong();
        return address.toString();
    }



    /**
     * Attempts to delete the provided file.<br>
     * Logging is performed on the result
     * @param file
     * @return
     */
    protected boolean deleteFile(File file) {
        boolean result = true;
        if(log.isTraceEnabled())
            log.trace("Attempting to delete file : "+file.getAbsolutePath());

        if(file != null && file.exists()) {
            try {
                result=file.delete();
                if(log.isTraceEnabled())
                    log.trace("Deleted file result: "+file.getAbsolutePath() +" : "+result);
            }
            catch(Throwable e) {
                log.error(Util.getMessage("FailedToDeleteFile") + file.getAbsolutePath(), e);
            }
        }
        return result;
    }


    protected static class Mapping implements Streamable {
        protected Address         logical_addr;
        protected Address         physical_addr;
        protected String          logical_name;

        public Mapping() {
        }

        public Mapping(Address logical_addr, PhysicalAddress physical_addr, String logical_name) {
            this.logical_addr=logical_addr;
            this.physical_addr=physical_addr;
            this.logical_name=logical_name;
        }

        public Address  getLogicalAddr()  {return logical_addr;}
        public Address  getPhysicalAddr() {return physical_addr;}
        public String   getLogicalName()  {return logical_name;}

        public void writeTo(DataOutput out) throws Exception {
            Util.writeAddress(logical_addr, out);
            Util.writeAddress(physical_addr, out);
            Bits.writeString(logical_name,out);
        }

        public void readFrom(DataInput in) throws Exception {
            logical_addr=Util.readAddress(in);
            physical_addr=Util.readAddress(in);
            logical_name=Bits.readString(in);
        }

        public String toString() {
            return logical_addr + ": " + physical_addr + " (logical name=" + logical_name + ")";
        }
    }
}
