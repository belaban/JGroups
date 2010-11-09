/**
 * Copyright (C) 2007 by Etrali SA. All rights reserved.
 */
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Vector;


public class ConnectChannelTest {
    private static final Log LOGGER=LogFactory.getLog(ConnectChannelTest.class);
    private final int nbExpectedHosts;
    private final int nbChannels;


    private final class ChannelHandler extends ReceiverAdapter {
        private final Vector<Address> members=new Vector<Address>();
        private final JChannel ch;
        private final String channel_name;
        private final Address local_addr;
        private long date;

        /**
         * Constructor.
         * @param name channel name
         * @param gms  GMS protocol layer
         * @param date starting date
         */
        public ChannelHandler(JChannel ch, long date) {
            this.ch=ch;
            this.date=date;
            this.channel_name=ch.getName();
            this.local_addr=ch.getAddress();
        }

        public void viewAccepted(View newView) {
            if(newView == null)
                return;

            Vector<Address> currentMembers=newView.getMembers();

            synchronized(members) {
                LOGGER.debug("Channel " + channel_name + " View accepted, coordinator status " + Util.isCoordinator(newView, local_addr));

                for(Address address : currentMembers) {
                    if(!members.contains(address)) {
                        LOGGER.debug("Channel " + channel_name + " Member joined " + address);
                    }
                }

                for(Address address : members) {
                    if(!currentMembers.contains(address)) {
                        LOGGER.debug("Channel " + channel_name + " Member left " + address);
                    }
                }

                members.removeAllElements();
                members.addAll(currentMembers);
            }

            if(newView instanceof MergeView) {
                LOGGER.error("Channel " + channel_name + " View merged " + newView);
            }

            if(members.size() == nbExpectedHosts) {
                long time=System.currentTimeMillis();
                LOGGER.info("Channel " + channel_name + " All expected members joined in " + (time - date) + " ms");
            }
        }
    }


    public ConnectChannelTest(int num_hosts, int num_channels) {
        nbExpectedHosts=num_hosts;
        nbChannels=num_channels;
    }


    private static void initializeJMX() {
        String port=System.getProperty("JMXPort", "5000");

        try {
            String host=InetAddress.getLocalHost().getHostName() + ":" + port;
            HashMap<String, Object> env=new HashMap<String, Object>();
            MBeanServer beanServer=ManagementFactory.getPlatformMBeanServer();

            LocateRegistry.createRegistry(Integer.parseInt(port));

            JMXServiceURL url=new JMXServiceURL(
                    "service:jmx:rmi://" + host + "/jndi/rmi://" + host + "/jmxrmi");

            JMXConnectorServer cs=JMXConnectorServerFactory.newJMXConnectorServer(url, env, beanServer);

            cs.start();
        }
        catch(Exception e) {
            LOGGER.error("Can not initialize JMX server", e);
        }
    }

    public boolean start() throws ChannelException {

        initializeJMX();

        long time, startingTime;

        LOGGER.info("Starting test with " + nbExpectedHosts + " expected members");

        for(int i=0; i < nbChannels; i++) {
            JChannel c=new JChannel();
            c.setName(String.valueOf(i));

            startingTime=System.currentTimeMillis();
            c.setReceiver(new ChannelHandler(c, System.currentTimeMillis()));

            try {
                c.connect("ConnectTestCluster");
            }
            catch(ChannelException e) {
                LOGGER.error("Can't connect channel", e);

                return false;
            }

            time=System.currentTimeMillis();

            LOGGER.info("Channel c" + i + " connected in " + (time - startingTime) + " ms");
        }

        return true;
    }

    /**
     * Main program.
     * @param args not used
     */
    public static void main(String[] args) throws ChannelException {
        int num_expected_hosts=Integer.parseInt(args[0]);
        int num_channels=Integer.parseInt(args[1]);
        
        final ConnectChannelTest test=new ConnectChannelTest(num_expected_hosts, num_channels);

        test.start();
    }
}
