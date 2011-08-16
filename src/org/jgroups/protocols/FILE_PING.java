package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.Property;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
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

        try {
            String filename=addr instanceof UUID? ((UUID)addr).toStringLong() : addr.toString();
            File file=new File(dir, filename + SUFFIX);
            if(log.isTraceEnabled())
                log.trace("removing " + file);
            file.delete();
        }
        catch(Throwable e) {
            log.error("failure removing data", e);
        }
    }

    /**
     * Reads all information from the given directory under clustername
     * @return
     */
   protected List<PingData> readAll(String clustername) {
        List<PingData> retval=new ArrayList<PingData>();
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File[] files=dir.listFiles(filter);
        if(files != null) {
            for(File file: files) {
                PingData data=readFile(file);
                if(data == null) {
                    log.warn("failed reading " + file.getName() + ": removing it");
                    file.delete();
                }
                else
                    retval.add(data);
            }
        }
        return retval;
    }

    protected static PingData readFile(File file) {
        PingData retval=null;
        DataInputStream in=null;

        try {
            in=new DataInputStream(new FileInputStream(file));
            PingData tmp=new PingData();
            tmp.readFrom(in);
            return tmp;
        }
        catch(Exception e) {
        }
        finally {
            Util.close(in);
        }
        return retval;
    }

    protected void writeToFile(PingData data, String clustername) {
        DataOutputStream out=null;
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        String filename=addressAsString(local_addr);
        File file=new File(dir, filename + SUFFIX);
        file.deleteOnExit();

        try {
            out=new DataOutputStream(new FileOutputStream(file));
            data.writeTo(out);
        }
        catch(Exception e) {
        }
        finally {
            Util.close(out);
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
        if (address == null) {
            return "";
        } else if (address instanceof UUID) {
            return ((UUID) address).toStringLong();
        } else {
            return address.toString();
        }
    }



}