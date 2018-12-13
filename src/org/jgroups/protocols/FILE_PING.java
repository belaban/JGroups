package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.util.Responses;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 */
public class FILE_PING extends Discovery {
    protected static final String SUFFIX=".list";
    protected static final Pattern regexp=Pattern.compile("[\0<>:\"/\\|?*]");

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    protected String location=File.separator + "tmp" + File.separator + "jgroups";

    @Deprecated @Property(description="Interval (in milliseconds) at which the own Address is written. 0 disables it.")
    protected long interval=60000;

    @Property(description="If true, on a view change, the new coordinator removes files from old coordinators")
    protected boolean remove_old_coords_on_view_change=false;

    @Property(description="If true, on a view change, the new coordinator removes all files except its own")
    protected boolean remove_all_files_on_view_change=false;

    @Property(description="The max number of times my own information should be written to the DB after a view change")
    protected int info_writer_max_writes_after_view=2;

    @Property(description="Interval (in ms) at which the info writer should kick in")
    protected long info_writer_sleep_time=10000;

    @Property(description="When a non-initial discovery is run, and InfoWriter is not running, write the data to " +
      "disk (if true). JIRA: https://issues.jboss.org/browse/JGRP-2288")
    protected boolean write_data_on_find;

    @ManagedAttribute(description="Number of writes to the file system or cloud store")
    protected int writes;

    @ManagedAttribute(description="Number of reads from the file system or cloud store")
    protected int reads;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File                        root_dir=null;
    protected static final FilenameFilter filter=new FilenameFilter() {
        public boolean accept(File dir, String name) {return name.endsWith(SUFFIX);}
    };
    protected Future<?>                   info_writer;

    public boolean   isDynamic()                             {return true;}
    public String    getLocation()                           {return location;}
    public FILE_PING setLocation(String l)                   {this.location=l; return this;}
    public boolean   getRemoveAllDataOnViewChange()          {return remove_all_files_on_view_change;}
    public FILE_PING setRemoveAllDataOnViewChange(boolean r) {remove_all_files_on_view_change=r; return this;}

    @ManagedAttribute(description="Whether the InfoWriter task is running")
    public synchronized boolean isInfoWriterRunning() {return info_writer != null && !info_writer.isDone();}

    @ManagedOperation(description="Causes the member to write its own information into the DB, replacing an existing entry")
    public void writeInfo() {if(is_coord) writeAll();}

    public void init() throws Exception {
        super.init();
        createRootDir();
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    remove(cluster_name, local_addr);
                }
            });
        }
        // This can be thrown if the JVM is already in the process of shutting down
        catch (IllegalStateException e) {
            log.debug("Unable to add shutdown hook for " + this.getClass().getCanonicalName() + ". File " + cluster_name + "/" + addressToFilename(local_addr) + " may not be deleted.");
        }
    }


    public void resetStats() {
        super.resetStats();
        reads=writes=0;
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
            case Event.DISCONNECT:
                remove(cluster_name, local_addr);
                break;
        }
        return super.down(evt);
    }

    public void findMembers(final List<Address> members, final boolean initial_discovery, Responses responses) {
        try {
            readAll(members, cluster_name, responses);
            if(responses.isEmpty()) {
                PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS,local_addr));
                PingData coord_data=new PingData(local_addr, true, UUID.get(local_addr), physical_addr).coord(is_coord);
                write(Collections.singletonList(coord_data), cluster_name);
                return;
            }
            if(!initial_discovery) {
                for(PingData data : responses)
                    handleDiscoveryResponse(data, data.sender);
            }
            PhysicalAddress phys_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            PingData data=responses.findResponseFrom(local_addr);
            // the logical addr *and* IP address:port have to match
            if(data != null && data.getPhysicalAddr().equals(phys_addr)) {
                if(data.isCoord() && initial_discovery)
                    responses.clear();
                else
                    ; // use case #1 if we have predefined files: most members join but are not coord
            }
            else {
                sendDiscoveryResponse(local_addr, phys_addr, UUID.get(local_addr), null, false);
            }
            // write the data if write_data_on_find is true
            if(write_data_on_find && (remove_all_files_on_view_change || remove_old_coords_on_view_change)
              && !initial_discovery && is_coord && (data == null || !data.isCoord()) && !isInfoWriterRunning()) {
                // a coordinator in a separate partition may have deleted this coordinator's file
                writeAll();
            }
        }
        finally {
            responses.done();
        }
    }



    /** Only add the discovery response if the logical address is not present or the physical addrs are different */
    protected boolean addDiscoveryResponseToCaches(Address mbr, String logical_name, PhysicalAddress physical_addr) {
        PhysicalAddress phys_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, mbr));
        boolean added=phys_addr == null || !phys_addr.equals(physical_addr);
        super.addDiscoveryResponseToCaches(mbr, logical_name, physical_addr);
        if(added && is_coord)
            writeAll();
        return added;
    }

    protected static String addressToFilename(Address mbr) {
        String logical_name=UUID.get(mbr);
        String name=(addressAsString(mbr) + (logical_name != null? "." + logical_name + SUFFIX : SUFFIX));
        return regexp.matcher(name).replaceAll("-");
    }

    protected void createRootDir() {
        root_dir=new File(location);
        if(root_dir.exists()) {
            if(!root_dir.isDirectory())
                throw new IllegalArgumentException("location " + root_dir.getPath() + " is not a directory");
        }
        else
            root_dir.mkdirs();
        if(!root_dir.exists())
            throw new IllegalArgumentException("location " + root_dir.getPath() + " could not be accessed");

    }

    // remove all files which are not from the current members
    protected void handleView(View new_view, View old_view, boolean coord_changed) {
        if(is_coord) {
            if(coord_changed) {
                if(remove_all_files_on_view_change)
                    removeAll(cluster_name);
                else if(remove_old_coords_on_view_change) {
                    Address old_coord=old_view != null? old_view.getCreator() : null;
                    if(old_coord != null)
                        remove(cluster_name, old_coord);
                }
            }
            if(coord_changed || View.diff(old_view, new_view)[1].length > 0) {
                writeAll();
                if(remove_all_files_on_view_change || remove_old_coords_on_view_change)
                    startInfoWriter();
            }
        }
        else if(coord_changed) // I'm no longer the coordinator
            remove(cluster_name, local_addr);
    }

    protected void remove(String clustername, Address addr) {
        if(clustername == null || addr == null)
            return;

        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            return;

        log.debug("remove %s", clustername);

        String filename=addressToFilename(addr);
        File file=new File(dir, filename);
        deleteFile(file);
    }

    /** Removes all files for the given cluster name */
    protected void removeAll(String clustername) {
        if(clustername == null)
            return;
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            return;
        File[] files=dir.listFiles(filter); // finds all files ending with '.list'
        for(File file: files)
            file.delete();
    }



    protected void readAll(List<Address> members, String clustername, Responses responses) {
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        File[] files=dir.listFiles(filter); // finds all files ending with '.list'
        for(File file: files) {
            List<PingData> list=null;
            // implementing a simple spin lock doing a few attempts to read the file
            // this is done since the file may be written in concurrency and may therefore not be readable
            for(int i=0; i < 3; i++) {
                if(file.exists()) {
                    try {
                        if((list=read(file)) != null)
                            break;
                    }
                    catch(Exception e) {
                    }
                }
                Util.sleep(50);
            }

            if(list == null) {
                log.warn("failed reading " + file.getAbsolutePath());
                continue;
            }
            for(PingData data: list) {
                if(members == null || members.contains(data.getAddress()))
                    responses.addResponse(data, true);
                if(local_addr != null && !local_addr.equals(data.getAddress()))
                    addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
            }
        }
    }

    // Format: [name] [UUID] [address:port] [coord (T or F)]. See doc/design/CloudBasedDiscovery.txt for details
    protected List<PingData> read(File file) throws Exception {
        return read(new FileInputStream(file));
    }

    @Override
    protected List<PingData> read(InputStream in) {
        try {
            return super.read(in);
        }
        finally {
            reads++;
        }
    }


    /** Write information about all of the member to file (only if I'm the coord) */
    protected void writeAll() {
        Map<Address,PhysicalAddress> cache_contents=
          (Map<Address,PhysicalAddress>)down_prot.down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS, false));

        List<PingData> list=new ArrayList<>(cache_contents.size());
        for(Map.Entry<Address,PhysicalAddress> entry: cache_contents.entrySet()) {
            Address         addr=entry.getKey();
            PhysicalAddress phys_addr=entry.getValue();
            PingData data=new PingData(addr, true, UUID.get(addr), phys_addr).coord(addr.equals(local_addr));
            list.add(data);
        }
        write(list, cluster_name);
        if(log.isTraceEnabled())
            log.trace("%s: wrote to backend store: %s", local_addr, list);
    }

    protected void write(List<PingData> list, String clustername) {
        File dir=new File(root_dir, clustername);
        if(!dir.exists())
            dir.mkdir();

        String filename=addressToFilename(local_addr);
        File destination=new File(dir, filename);

        try {
            write(list, new FileOutputStream(destination));
        }
        catch(Exception ioe) {
            log.error(Util.getMessage("AttemptToWriteDataFailedAt") + clustername + " : " + destination.getName(), ioe);
            deleteFile(destination);
        }
    }


    protected void write(List<PingData> list, OutputStream out) throws Exception {
        try {
            super.write(list, out);
        }
        finally {
            writes++;
        }
    }



    protected boolean deleteFile(File file) {
        boolean result = true;
        if(log.isTraceEnabled())
            log.trace("Attempting to delete file : "+file.getAbsolutePath());

        if(file != null && file.exists()) {
            try {
                result=file.delete();
                log.trace("Deleted file result: "+file.getAbsolutePath() +" : "+result);
            }
            catch(Throwable e) {
                log.error(Util.getMessage("FailedToDeleteFile") + file.getAbsolutePath(), e);
            }
        }
        return result;
    }

    protected synchronized void startInfoWriter() {
        if(info_writer == null || info_writer.isDone())
            info_writer=timer.scheduleWithDynamicInterval(new InfoWriter(info_writer_max_writes_after_view, info_writer_sleep_time));
    }

    protected synchronized void stopInfoWriter() {
        if(info_writer != null)
            info_writer.cancel(false);
    }


    /** Class which calls writeAll() a few times. Started after a view change in which an old coord left */
    protected class InfoWriter implements TimeScheduler.Task {
        protected final int  max_writes;
        protected int        num_writes;
        protected final long sleep_interval;

        public InfoWriter(int max_writes, long sleep_interval) {
            this.max_writes=max_writes;
            this.sleep_interval=sleep_interval;
        }

        @Override
        public long nextInterval() {
            if(++num_writes > max_writes)
                return 0; // discontinues this task
            return Math.max(1000, Util.random(sleep_interval));
        }

        @Override
        public void run() {
            writeAll();
        }
    }


}
