package org.jgroups.util;

import java.io.*;
import java.util.*;

/**
 * Maintains a list of ports used on this host, associated with time stamps. The ports are persisted into the
 * temp file system.
 * @author Bela Ban
 * @version $Id: PortsManager.java,v 1.5 2009/01/05 07:08:41 belaban Exp $
 */
public class PortsManager {
    private String filename="jgroups-ports.txt";
    private String temp_dir=System.getProperty("java.io.tmpdir", "/tmp");
    private final static String file_separator=System.getProperty("file.separator", "/");
    private String absolute_name=temp_dir + file_separator + filename;

    /** Time after which a port can be removed and re-allocated */
    private long expiry_time=60 * 1000L;

    public PortsManager() {
    }

    public PortsManager(long expiry_time) {
        this.expiry_time=expiry_time;
    }

    public PortsManager(String ports_file) {
        if(ports_file != null)
            this.absolute_name=ports_file;
    }

    public PortsManager(long expiry_time, String ports_file) {
        this.expiry_time=expiry_time;
        if(ports_file != null)
            absolute_name=ports_file;
    }
    
    public PortsManager(long expiry_time, String filename, String temp_dir) {
        this.expiry_time=expiry_time;
        this.filename=filename;
        this.temp_dir=temp_dir;
        absolute_name=temp_dir + file_separator + filename;
    }


    public long getExpiryTime() {
        return expiry_time;
    }

    public void setExpiryTime(long expiry_time) {
        this.expiry_time=expiry_time;
    }

    /** Loads the file, weeds out expired ports, returns the next available port and saves the new port in the file */
    public int getNextAvailablePort(int start_port) {
        Map<Integer,Long> map;
        int retval=-1;

        try {
            map=load();
            long current_time=System.currentTimeMillis(), timestamp;
            if(map.isEmpty()) {
                map.put(start_port, current_time);
                store(map);
                return start_port;
            }

            // wed out expired ports
            Map.Entry<Integer,Long> entry;
            for(Iterator<Map.Entry<Integer,Long>> it=map.entrySet().iterator(); it.hasNext();) {
                entry=it.next();
                timestamp=entry.getValue();
                if(current_time - timestamp >= expiry_time) {
                    it.remove();
                }
            }

            // find next available port
            Set<Integer> ports=map.keySet();
            while(ports.contains(start_port)) {
                start_port++;
            }
            map.put(start_port, System.currentTimeMillis());
            store(map);
            return start_port;
        }
        catch(IOException e) {
            return retval;
        }
    }

    /** Loads the file, removes the port (if existent) and closes the file again */
    public void removePort(int port) {
        Map<Integer,Long> map;

        try {
            map=load();
            if(map.isEmpty()) {
                return;
            }
            map.remove(port);
            store(map);
        }
        catch(IOException e) {
        }
    }

    /** Deletes the underlying file. Used for unit testing, not recommended for regular use ! */
    public void deleteFile() {
        File file=new File(absolute_name);
        file.delete();
    }


    private Map<Integer,Long> load() throws IOException {
        InputStream in=null;
        Map<Integer,Long> retval=new HashMap<Integer,Long>();
        try {
            in=new FileInputStream(absolute_name);
            Properties props=new Properties();
            props.load(in);
            for(Iterator<Map.Entry<Object,Object>> it=props.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Object,Object> entry=it.next();
                String keystr=(String)entry.getKey(), valstr=(String)entry.getValue();
                int key=Integer.parseInt(keystr);
                long val=Long.parseLong(valstr);
                retval.put(key, val);
            }
            return retval;
        }
        catch(Throwable t) {
            return retval;
        }
        finally {
            Util.close(in);
        }
    }

    private void store(Map<Integer,Long> map) throws IOException {
        OutputStream out=null;
        try {
            out=new FileOutputStream(absolute_name);
            Properties props=new Properties();
            for(Map.Entry<Integer,Long> entry: map.entrySet()) {
                String key=entry.getKey().toString();
                String val=entry.getValue().toString();
                props.put(key, val);
            }
            props.store(out, "Persistent JGroups ports");
        }
        finally {
            Util.close(out);
        }
    }
}
