package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.util.NameCache;
import org.jgroups.util.Responses;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


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
    protected String  location=File.separator + "tmp" + File.separator + "jgroups";

    @Property(description="If true, on a view change, the new coordinator removes files from old coordinators")
    protected boolean remove_old_coords_on_view_change;

    @Property(description="If true, on a view change, the new coordinator removes all data except its own")
    protected boolean remove_all_data_on_view_change;

    @Property(description="The max number of times my own information should be written to the storage after a view change")
    protected int     info_writer_max_writes_after_view=2;

    @Property(description="Interval (in ms) at which the info writer should kick in",type=AttributeType.TIME)
    protected long    info_writer_sleep_time=10000;

    @Property(description="When a non-initial discovery is run, and InfoWriter is not running, write the data to " +
      "disk (if true). JIRA: https://issues.jboss.org/browse/JGRP-2288")
    protected boolean write_data_on_find;

    @Property(description = "If set, a shutdown hook is registered with the JVM to remove the local address "
      + "from the store. Default is true", writable = false)
    protected boolean register_shutdown_hook = true;

    @Property(description="Change the backend store when the view changes. If off, then the file is only changed on " +
      "joins, but not on leaves. Enabling this will increase traffic to the backend store.")
    protected boolean update_store_on_view_change=true;

    @ManagedAttribute(description="Number of writes to the file system or cloud store")
    protected int     writes;

    @ManagedAttribute(description="Number of reads from the file system or cloud store")
    protected int     reads;


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File                        root_dir=null;
    protected static final FilenameFilter filter=(dir, name1) -> name1.endsWith(SUFFIX);
    protected Future<?>                   info_writer;

    public boolean   isDynamic()                          {return true;}
    public String    getLocation()                        {return location;}
    public FILE_PING setLocation(String l)                {this.location=l; return this;}
    public boolean   removeAllDataOnViewChange()          {return remove_all_data_on_view_change;}
    public FILE_PING removeAllDataOnViewChange(boolean r) {remove_all_data_on_view_change=r; return this;}

    public boolean   removeOldCoordsOnViewChange() {return remove_old_coords_on_view_change;}
    public FILE_PING removeOldCoordsOnViewChange(boolean r) {this.remove_old_coords_on_view_change=r; return this;}

    public int       getInfoWriterMaxWritesAfterView() {return info_writer_max_writes_after_view;}
    public FILE_PING setInfoWriterMaxWritesAfterView(int i) {this.info_writer_max_writes_after_view=i; return this;}

    public long      getInfoWriterSleepTime() {return info_writer_sleep_time;}
    public FILE_PING setInfoWriterSleepTime(long i) {this.info_writer_sleep_time=i; return this;}

    public boolean   writeDataOnFind() {return write_data_on_find;}
    public FILE_PING writeDataOnFind(boolean w) {this.write_data_on_find=w; return this;}

    public boolean   registerShutdownHook() {return register_shutdown_hook;}
    public FILE_PING registerShutdownHook(boolean r) {this.register_shutdown_hook=r; return this;}

    public boolean   updateStoreOnViewChange() {return update_store_on_view_change;}
    public FILE_PING updateStoreOnViewChange(boolean u) {this.update_store_on_view_change=u; return this;}




    @ManagedAttribute(description="Whether the InfoWriter task is running")
    public synchronized boolean isInfoWriterRunning() {return info_writer != null && !info_writer.isDone();}

    @ManagedOperation(description="Causes the member to write its own information into the DB, replacing an existing entry")
    public void writeInfo() {if(is_coord) writeAll();}

    public void init() throws Exception {
        super.init();
        createRootDir();
        if(register_shutdown_hook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> remove(cluster_name, local_addr)));
        }
    }

    public void stop() {
        super.stop();
        stopInfoWriter();
        remove(cluster_name, local_addr);
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
                View new_view=evt.getArg();
                handleView(new_view, old_view, previous_coord != is_coord);
                return retval;
        }
        return super.down(evt);
    }

    public void findMembers(final List<Address> members, final boolean initial_discovery, Responses responses) {
        try {
            readAll(members, cluster_name, responses);
            if(responses.isEmpty()) {
                PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS,local_addr));
                PingData coord_data=new PingData(local_addr, true, NameCache.get(local_addr), physical_addr).coord(is_coord);
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
                sendDiscoveryResponse(local_addr, phys_addr, NameCache.get(local_addr), null, is_coord);
            }
            // write the data if write_data_on_find is true
            if(write_data_on_find && (remove_all_data_on_view_change || remove_old_coords_on_view_change)
              && !initial_discovery && is_coord && (data == null || !data.isCoord()) && !isInfoWriterRunning()) {
                // a coordinator in a separate partition may have deleted this coordinator's file
                writeAll();
            }
        }
        finally {
            responses.done();
        }
    }



    protected static String addressToFilename(Address mbr) {
        String logical_name=NameCache.get(mbr);
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
            if(remove_all_data_on_view_change)
                removeAll(cluster_name);
            else if(remove_old_coords_on_view_change) {
                Address old_coord=old_view != null? old_view.getCreator() : null;
                if(old_coord != null)
                    remove(cluster_name, old_coord);
            }
            Address[] left=View.diff(old_view, new_view)[1];
            if(coord_changed || update_store_on_view_change || left.length > 0) {
                writeAll(left);
                if(remove_all_data_on_view_change || remove_old_coords_on_view_change)
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
        writeAll(null);
    }

    protected void writeAll(Address[] excluded_mbrs) {
        Map<Address,PhysicalAddress> cache_contents=
          (Map<Address,PhysicalAddress>)down_prot.down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS, true));

        if(excluded_mbrs != null)
            for(Address excluded_mbr : excluded_mbrs)
                cache_contents.remove(excluded_mbr);

        List<PingData> list=new ArrayList<>(cache_contents.size());
        for(Map.Entry<Address,PhysicalAddress> entry: cache_contents.entrySet()) {
            Address         addr=entry.getKey();
            if(update_store_on_view_change && (view != null && !view.containsMember(addr)))
                continue;
            PhysicalAddress phys_addr=entry.getValue();
            PingData data=new PingData(addr, true, NameCache.get(addr), phys_addr).coord(addr.equals(local_addr));
            list.add(data);
        }
        write(list, cluster_name);
        log.trace("%s: wrote to backend store: %s", local_addr, list.stream().map(PingData::getAddress).collect(Collectors.toList()));
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

        if(file != null && file.exists()) {
            try {
                result=file.delete();
                log.trace("deleted file %s: result=%b ", file.getAbsolutePath(), result);
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
