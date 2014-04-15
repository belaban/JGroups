package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.util.Responses;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.List;
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
    protected File                        root_dir=null;
    protected static final FilenameFilter filter=new FilenameFilter() {
        public boolean accept(File dir, String name) {return name.endsWith(SUFFIX);}
    };
    protected Future<?>                   writer_future;
    protected volatile View               prev_view;


    public void init() throws Exception {
        super.init();
        createRootDir();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                remove(cluster_name, local_addr);
            }
        });
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

    public void findMembers(final List<Address> members, final boolean initial_discovery, Responses responses) {
        readAll(members, cluster_name, responses);
        writeOwnInformation();
    }


    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.VIEW_CHANGE:
                View old_view=view;
                boolean previous_coord=is_coord;
                Object retval=super.down(evt);
                View new_view=(View)evt.getArg();
                handleView(new_view, old_view, previous_coord != is_coord);
                return retval;
        }
       return super.down(evt);
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

    }

    // remove all files which are not from the current members
    protected void handleView(View new_view, View old_view, boolean coord_changed) {
        if(is_coord && old_view != null && new_view != null) {
            Address[][] diff=View.diff(old_view, new_view);
            Address[] left_mbrs=diff[1];
            for(Address left_mbr: left_mbrs)
                if(left_mbr != null && !new_view.containsMember(left_mbr))
                    remove(cluster_name, left_mbr);
        }
        if(coord_changed)
            writeOwnInformation();
    }

    protected void remove(String clustername, Address addr) {
        if(clustername == null || addr == null)
            return;

        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            return;

        log.debug("remove %s", clustername);

        String filename=addr instanceof UUID? ((UUID)addr).toStringLong() : addr.toString();
        File file=new File(dir, filename + SUFFIX);
        deleteFile(file);
    }

    /**
     * Reads all information from the given directory under clustername
     */
    protected synchronized void readAll(List<Address> members, String clustername, Responses responses) {
        File dir=new File(root_dir,clustername);
        if(!dir.exists())
            dir.mkdir();

        File[] files=listFiles(dir, members);
        for(File file: files) {
            PingData data=null;
            // implementing a simple spin lock doing a few attempts to read the file
            // this is done since the file may be written in concurrency and may therefore not be readable
            for(int i=0; i < 3; i++) {
                if(file.exists()) {
                    try {
                        if((data=readFile(file)) != null)
                            break;
                    }
                    catch(Exception e) {
                    }
                }
                Util.sleep(50);
            }

            if(data == null) {
                log.warn("failed reading " + file.getAbsolutePath() + ": removing it from " + clustername);
                deleteFile(file);
                continue;
            }
            responses.addResponse(data, true);
            if(local_addr != null && !local_addr.equals(data.getAddress()))
                addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
        }
    }

    protected static File[] listFiles(File dir, List<Address> members) {
        if(members == null || members.isEmpty())
            return dir.listFiles(filter);
        File[] files=new File[members.size()];
        int index=0;
        for(Address mbr: members) {
            String filename=addressAsString(mbr) + SUFFIX;
            File file=new File(dir, filename);
            files[index++]=file;
        }
        return files;
    }

    public static synchronized PingData readFile(File file) throws Exception {
        DataInputStream in=null;
        try {
            in=new DataInputStream(new FileInputStream(file));
            PingData tmp=new PingData();
            tmp.readFrom(in);
            return tmp;
        }
        finally {
            Util.close(in);
        }
    }

    /** Write my own UUID,logical name and physical address to a file */
    protected void writeOwnInformation() {
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS,local_addr));
        PingData data=new PingData(local_addr, is_server, UUID.get(local_addr), physical_addr).coord(is_coord);
        writeToFile(data,cluster_name); // write my own data to file
    }

    protected synchronized void writeToFile(PingData data, String clustername) {
        File dir=new File(root_dir,clustername);
        if(!dir.exists())
            dir.mkdir();

        if(data == null) {
            return;
        }

        String filename=addressAsString(local_addr);

        // write all data to a temporary file; this is because the writing can be very slow under some circumstances
        File tmpFile=writeToTempFile(dir, data);
        if(tmpFile == null)
            return;

        File destination=new File(dir, filename + SUFFIX);
        FileChannel src_ch=null, dest_ch=null;
        try {
            // do a file move, this is much faster and could be considered atomic on most operating systems
            src_ch=new FileInputStream(tmpFile).getChannel();
            dest_ch=new FileOutputStream(destination).getChannel();
            src_ch.transferTo(0, src_ch.size(), dest_ch);
        }
        catch(IOException ioe) {
            log.error("attempt to move failed at " + clustername + " : " + tmpFile.getName() + "->" + destination.getName(),ioe);
        }
        finally {
            Util.close(src_ch, dest_ch);
            deleteFile(tmpFile);
        }
    }

    protected class WriterTask implements Runnable {
        public void run() {
            writeOwnInformation();
        }
    }
    
    protected static String addressAsString(Address address) {
        if(address == null)
            return "";
        if(address instanceof UUID)
            return ((UUID) address).toStringLong();
        return address.toString();
    }

    private boolean deleteFile(File file) {
        boolean result = true;
        if(log.isTraceEnabled())
            log.trace("Attempting to delete file : "+file.getAbsolutePath());

        if(file != null && file.exists()) {
            try {
                result=file.delete();
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