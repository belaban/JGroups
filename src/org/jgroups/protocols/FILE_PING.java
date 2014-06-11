package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.PhysicalAddress;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Responses;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Simple discovery protocol which uses a file on shared storage such as an SMB share, NFS mount or S3. The local
 * address information, e.g. UUID and physical addresses mappings are written to the file and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/FILE_PING.txt
 * @author Bela Ban
 */
public class FILE_PING extends Discovery {
    protected static final String SUFFIX=".list";

    /* -----------------------------------------    Properties     -------------------------------------------------- */


    @Property(description="The absolute path of the shared file")
    protected String location=File.separator + "tmp" + File.separator + "jgroups";

    @Deprecated @Property(description="Interval (in milliseconds) at which the own Address is written. 0 disables it.")
    protected long interval=60000;

    @ManagedAttribute(description="Number of writes to the file system or cloud store")
    protected int writes;

    @ManagedAttribute(description="Number of reads from the file system or cloud store")
    protected int reads;

    protected static final byte[] WHITESPACE=" \t".getBytes();


    /* --------------------------------------------- Fields ------------------------------------------------------ */
    protected File                        root_dir=null;
    protected static final FilenameFilter filter=new FilenameFilter() {
        public boolean accept(File dir, String name) {return name.endsWith(SUFFIX);}
    };
    protected volatile View               prev_view;

    public boolean isDynamic() {return true;}

    public void init() throws Exception {
        super.init();
        createRootDir();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                remove(cluster_name, local_addr);
            }
        });
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
                write(Arrays.asList(coord_data), cluster_name);
                return;
            }

            PhysicalAddress phys_addr=(PhysicalAddress)down_prot.down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
            PingData data=responses.findResponseFrom(local_addr);
            // the logical addr *and* IP address:port have to match
            if(data != null && data.getPhysicalAddr().equals(phys_addr)) {
                if(data.isCoord())
                    responses.clear();
                else
                    ; // use case #1 if we have predefined files: most members join but are not coord
            }
            else {
                sendDiscoveryResponse(local_addr, phys_addr, UUID.get(local_addr), null, false);
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
        return addressAsString(mbr) + (logical_name != null? "." + logical_name + SUFFIX : SUFFIX);
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
        if(coord_changed) {
            if(is_coord)
                writeAll();
            else
                remove(cluster_name, local_addr);
        }
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


    protected List<PingData> read(InputStream in) throws Exception {
        List<PingData> retval=null;
        try {
            while(true) {
                String name_str=Util.readToken(in);
                String uuid_str=Util.readToken(in);
                String addr_str=Util.readToken(in);
                String coord_str=Util.readToken(in);
                if(name_str == null || uuid_str == null || addr_str == null || coord_str == null)
                    break;

                UUID uuid=null;
                try {
                    long tmp=Long.valueOf(uuid_str);
                    uuid=new UUID(0, tmp);
                }
                catch(Throwable t) {
                    uuid=UUID.fromString(uuid_str);
                }

                PhysicalAddress phys_addr=new IpAddress(addr_str);
                boolean is_coordinator=coord_str.trim().equals("T") || coord_str.trim().equals("t");

                if(retval == null)
                    retval=new ArrayList<PingData>();
                retval.add(new PingData(uuid, true, name_str, phys_addr).coord(is_coordinator));
            }
            return retval;
        }
        finally {
            Util.close(in);
            reads++;
        }
    }


    /** Write information about all of the member to file (only if I'm the coord) */
    protected void writeAll() {
        Map<Address,PhysicalAddress> cache_contents=
          (Map<Address,PhysicalAddress>)down_prot.down(new Event(Event.GET_LOGICAL_PHYSICAL_MAPPINGS, false));

        List<PingData> list=new ArrayList<PingData>(cache_contents.size());
        for(Map.Entry<Address,PhysicalAddress> entry: cache_contents.entrySet()) {
            Address         addr=entry.getKey();
            PhysicalAddress phys_addr=entry.getValue();
            PingData data=new PingData(addr, true, UUID.get(addr), phys_addr).coord(addr.equals(local_addr));
            list.add(data);
        }
        write(list, cluster_name);
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
            log.error("attempt to write data failed at " + clustername + " : " + destination.getName(), ioe);
            deleteFile(destination);
        }
    }


    protected void write(List<PingData> list, OutputStream out) throws Exception {
        try {
            for(PingData data: list) {
                String  logical_name=data.getLogicalName();
                Address addr=data.getAddress();
                PhysicalAddress phys_addr=data.getPhysicalAddr();
                if(logical_name == null || addr == null || phys_addr == null)
                    continue;
                out.write(logical_name.getBytes());
                out.write(WHITESPACE);

                out.write(addressAsString(addr).getBytes());
                out.write(WHITESPACE);

                out.write(phys_addr.toString().getBytes());
                out.write(WHITESPACE);

                out.write(data.isCoord()? "T\n".getBytes() : "F\n".getBytes());
            }
        }
        finally {
            Util.close(out);
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
                log.error("Failed to delete file: " + file.getAbsolutePath(), e);
            }
        }
        return result;
    }


}