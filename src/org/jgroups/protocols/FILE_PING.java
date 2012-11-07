package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 */
public class FILE_PING extends Discovery {
    protected static final String SUFFIX=".node";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    protected String location=File.separator + "tmp" + File.separator + "jgroups";

    @Property(description="Interval (in milliseconds) at which the own Address is written. 0 disables it.")
    protected long interval=60000;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File root_dir=null;
    protected FilenameFilter filter;
    private Future<?> writer_future;


    public void init() throws Exception {
        super.init();
        createRootDir();
    }

    public void start() throws Exception {
        super.start();
        if(interval > 0)
            writer_future=timer.scheduleWithFixedDelay(new WriterTask(), interval, interval, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if(writer_future != null) {
            writer_future.cancel(false);
            writer_future=null;
        }
        super.stop();
    }

    public boolean isDynamic() {
        return true;
    }

    public Collection<PhysicalAddress> fetchClusterMembers(String cluster_name) {
        List<PingData> existing_mbrs=readAll(cluster_name);

        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
        writeToFile(data, cluster_name); // write my own data to file

        // If we don't find any files, return immediately
        if(existing_mbrs.isEmpty())
            return Collections.emptyList();

        Set<PhysicalAddress> retval=new HashSet<PhysicalAddress>();

        for(PingData tmp: existing_mbrs) {
            Collection<PhysicalAddress> dests=tmp != null? tmp.getPhysicalAddrs() : null;
            if(dests == null)
                continue;
            for(final PhysicalAddress dest: dests) {
                if(dest == null)
                    continue;
                retval.add(dest);
            }
        }

        return retval;
    }

    public boolean sendDiscoveryRequestsInParallel() {
        return true;
    }


    public Object down(Event evt) {
        Object retval=super.down(evt);
        if(evt.getType() == Event.VIEW_CHANGE)
            handleView((View)evt.getArg());
        return retval;
    }

    protected void createRootDir() {
        root_dir=new File(location);
        if(root_dir.exists()) {
            if(!root_dir.isDirectory())
                throw new IllegalArgumentException("location " + root_dir.getPath() + " is not a directory");
        }
        else {
            root_dir.mkdirs();
        }
        if(!root_dir.exists())
            throw new IllegalArgumentException("location " + root_dir.getPath() + " could not be accessed");

        filter=new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(SUFFIX);
            }
        };
    }

    // remove all files which are not from the current members
    protected void handleView(View view) {
        Collection<Address> mbrs=view.getMembers();
        boolean is_coordinator=!mbrs.isEmpty() && mbrs.iterator().next().equals(local_addr);
        if(is_coordinator) {
            List<PingData> data=readAll(group_addr);
            for(PingData entry: data) {
                Address addr=entry.getAddress();
                if(addr != null && !mbrs.contains(addr)) {
                    remove(group_addr, addr);
                }
            }
        }
    }

    protected void remove(String clustername, Address addr) {
        if(clustername == null || addr == null)
            return;

        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            return;

        if(log.isDebugEnabled()) {
        	log.debug("remove : "+clustername);
        }
        
        String filename=addr instanceof UUID? ((UUID)addr).toStringLong() : addr.toString();
        File file=new File(dir, filename + SUFFIX);
        deleteFile(file);
    }

    /**
     * Reads all information from the given directory under clustername
     * @return
     */
    protected synchronized List<PingData> readAll(String clustername) {
        List<PingData> retval=new ArrayList<PingData>();
        File dir=new File(root_dir,clustername);
        if(!dir.exists())
            dir.mkdir();

        if(log.isDebugEnabled())
            log.debug("reading all : " + clustername);
        File[] files=dir.listFiles(filter);
        if(files != null) {
            for(File file: files) {
                PingData data=null;
                //implementing a simple spin lock doing a few attempts to read the file
                //this is done since the file may be written in concurrency and may therefore not be readable
                for(int i=0; i < 3; i++) {
                    data=null;
                    if(file.exists())
                        data=readFile(file);
                    if(data != null)
                        break;
                    else
                        Util.sleep(100);
                }

                if(data == null) {
                    log.warn("failed parsing content in " + file.getAbsolutePath() + ": removing it from " + clustername);
                    deleteFile(file);
                }
                else
                    retval.add(data);
            }
        }
        return retval;
    }

    private synchronized PingData readFile(File file) {
        PingData retval=null;
        DataInputStream in=null;

        try {
            in=new DataInputStream(new FileInputStream(file));
            PingData tmp=new PingData();
            tmp.readFrom(in);
            return tmp;
        }
        catch(Exception e) {
            log.debug("failed to read file : "+file.getAbsolutePath(), e);
        }
        finally {
            Util.close(in);
        }
        return retval;
    }

    protected synchronized void writeToFile(PingData data, String clustername) {
        File dir=new File(root_dir,clustername);
        if(!dir.exists())
            dir.mkdir();

        if(data == null) {
            return;
        }

        String filename=addressAsString(local_addr);

        //first write all data to a temporary file
        //this is because the writing can be very slow under some circumstances
        File tmpFile=writeToTempFile(dir, data);
        if(tmpFile == null)
            return;

        File destination=new File(dir, filename + SUFFIX);
        try {
            //do a file move, this is much faster and could be considered atomic on most operating systems
            FileChannel src_ch=new FileInputStream(tmpFile).getChannel();
            FileChannel dest_ch=new FileOutputStream(destination).getChannel();
            src_ch.transferTo(0,src_ch.size(),dest_ch);
            src_ch.close();
            dest_ch.close();
            if(log.isTraceEnabled())
                log.trace("Moved: " + tmpFile.getName() + "->" + destination.getName());
        }
        catch(IOException ioe) {
            log.error("attempt to move failed at " + clustername + " : " + tmpFile.getName() + "->" + destination.getName(),ioe);
        }
        finally {
            deleteFile(tmpFile);
        }
    }

    protected class WriterTask implements Runnable {
        public void run() {
            PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            List<PhysicalAddress> physical_addrs=Arrays.asList(physical_addr);
            PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);
            writeToFile(data, group_addr);
        }
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
    private boolean deleteFile(File file) {
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
                log.error("Failed to delete file: " + file.getAbsolutePath(), e);
            }
        }
        return result;
    }
       

    /**
     * Writes the data to a temporary file.<br>
     * The file is stored in the same directory as the other cluster files but is given the <tt>.tmp</tmp> suffix 
     * @param dir The cluster file dir
     * @param data the data to write
     * @return
     */
    private File writeToTempFile(File dir, PingData data) {
        DataOutputStream out=null;
               
        String filename=addressAsString(local_addr);
        File file=new File(dir, filename + ".tmp");
                
        try {
            out=new DataOutputStream(new FileOutputStream(file));
            data.writeTo(out);
            Util.close(out);
            if(log.isTraceEnabled())
                log.trace("Stored temporary file: "+file.getAbsolutePath());            	
        }
        catch(Exception e) {
            Util.close(out);
        	log.error("Failed to write temporary file: "+file.getAbsolutePath(), e);
        	deleteFile(file);
        	return null;
        }
		return file;
    }


}