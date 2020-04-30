package org.jgroups.protocols;

import org.jgroups.JChannelProbeHandler;
import org.jgroups.Version;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.jgroups.stack.DiagnosticsHandler.ProbeHandler;

/**
 * Periodically fetches some attributes and writes them to a file (https://issues.redhat.com/browse/JGRP-2402)
 * @author Bela Ban
 * @since  4.2.2, 5.0.0
 */
@MBean(description="Periodically fetches some attributes from selected (use-configurable) protocols and writes them to a file")
public class SOS extends Protocol {

    @Property(description="File to which the periodic data is written",writable=false)
    protected String            filename="${sos.filename:jgroups.sos}";

    @Property(description="Interval in ms at which the attributes are fetched and written to the file",
      writable=false,type=AttributeType.TIME)
    protected long              interval=60_000 * 15;

    @Property(description="The attributes to be fetched. In probe format ('jmx' or 'op' command)",writable=false)
    protected String            cmd="jmx=TP.bind_,thread_pool_ jmx=FD_ALL3.num_s op=TP.printLogicalAddressCache";

    @Property(description="The configuration file containing all protocols and attributes to be dumped")
    protected String            config="sos.cfg";

    protected Set<ProbeHandler> handlers;
    private Future<?>           task;


    public String getFilename()           {return filename;}
    public SOS    setFileName(String f)   {filename=f; return this;}
    public long   getInterval()           {return interval;}
    public SOS    setInterval(long i)     {interval=i; return this;}

    @ManagedOperation(description="Reads the contents of the given file and sets cmd")
    public SOS setCommand(String filename) throws IOException {
        cmd=Util.readFile(filename);
        return this;
    }

    @ManagedOperation(description="Reads the attributes to be dumped from the default configuration file")
    public SOS read() throws IOException {
        try(InputStream input=getInput(config)) {
            cmd=Util.readContents(input);
        }
        return this;
    }

    public void init() throws Exception {
        super.init();
        read();
    }

    public void start() throws Exception {
        super.start();
        TP tp=getTransport();
        if(tp.getDiagnosticsHandler() != null)
            handlers=tp.getDiagnosticsHandler().getProbeHandlers();
        else
            handlers=Collections.singleton(new JChannelProbeHandler(stack.getChannel()));
        task=tp.getTimer().scheduleWithFixedDelay(new DumperTask(), interval, interval, TimeUnit.MILLISECONDS, false);
    }

    public void stop() {
        super.stop();
        task.cancel(true);
    }

    @ManagedOperation(description="Dumps attributes / invokes operations from given protocols")
    public String exec() {
        StringTokenizer t=new StringTokenizer(cmd);
        List<String> list=new ArrayList<>();
        while(t.hasMoreTokens())
            list.add(t.nextToken());

        String[] args=list.toArray(new String[]{});
        StringBuilder sb=new StringBuilder(getMetadata());
        for(DiagnosticsHandler.ProbeHandler ph: handlers) {
            Map<String,String> ret=ph.handleProbe(args);
            if(ret != null && !ret.isEmpty())
                for(Map.Entry<String,String> e: ret.entrySet())
                    sb.append(String.format("\n* %s: %s", e.getKey(), e.getValue()));
        }
        return sb.append("\n").toString();
    }

    protected String getMetadata() {
        TP tp=stack.getTransport();
        return String.format("\nDate: %s, member: %s (%s), version: %s\nview: %s\n",
                             new Date(), tp.getLocalAddress(), tp.getPhysicalAddress(),
                             Version.printVersion(), tp.view());
    }

    protected InputStream getInput(String name) throws FileNotFoundException {
        InputStream input=Util.getResourceAsStream(name, getClass());
        if(input == null)
            input=new FileInputStream(name);
        if(input == null)
            throw new IllegalArgumentException(String.format("config file %s not found", name));
        return input;
    }

    protected class DumperTask implements Runnable {

        public void run() {
            try(OutputStream out=new FileOutputStream(filename)) {
                String dump=exec();
                out.write(dump.getBytes());
            }
            catch(Exception e) {
                log.error("%s: failed dumping SOS information to %s: %s", getTransport().getLocalAddress(), filename, e);
            }
        }

        public String toString() {
            return String.format("%s: %s (%s)", SOS.class.getSimpleName(), getClass().getSimpleName(),
                                 getTransport().getLocalAddress());
        }
    }
}
