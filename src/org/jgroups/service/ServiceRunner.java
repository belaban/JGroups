package org.jgroups.service;

import org.jgroups.Channel;
import org.jgroups.JChannel;

import java.util.ResourceBundle;

/**
 * <code>ServiceRunner</code> is utility class that runs services in standalone
 * mode. Each service is described with resource file containing following
 * properties:
 * <p/>
 * <ul>
 * <li><code>serviceClass</code> - class name of service to run. Each service
 * must define public constructor that takes two parameters, instances of
 * {@link org.jgroups.Channel} class;
 * <p/>
 * <li><code>serviceChannel</code> - string description of protocol stack for
 * inter-service communication channel;
 * <li><code>serviceGroup</code> - group name of inter-service communication
 * channel;
 * <p/>
 * <li><code>clientChannel</code> - protocol stack for client communication
 * channel;
 * <p/>
 * <li><code>clientGroup</code> - group name of client communication channel.
 * </ul>
 * <p/>
 * Class can be started from command line using:
 * <pre>
 * java org.jgroups.service.ServiceRunner -res <res_name>
 * </pre>
 * where <code>res_name</code> is name of the resource describing service to
 * run in form acceptable by {@link java.util.ResourceBundle} class.
 *
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 */
public class ServiceRunner {
    public static final String SERVICE_CLASS="serviceClass";

    public static final String SERVICE_CHANNEL_STACK="serviceChannel";

    public static final String SERVICE_GROUP_NAME="serviceGroup";

    public static final String CLIENT_CHANNEL_STACK="clientChannel";

    public static final String CLIENT_GROUP_NAME="clientGroup";

    public static final String HELP_SWITCH="-help";

    public static final String RESOURCE_SWITCH="-res";

    /**
     * Method to start service. This method extracts parameters from
     * specified resource, creates instance of service and starts it.
     *
     * @param res resource bundle containing information about resource.
     */
    public static void startService(ResourceBundle res) throws Exception {

        String className=res.getString(SERVICE_CLASS);

        if(className == null || "".equals(className)) {
            System.out.println("Specified resource does not contain service class name");
            System.exit(1);
        }

        Class serviceClass=Class.forName(className);

        if(!AbstractService.class.isAssignableFrom(serviceClass)) {
            System.out.println("Specified service class is not instance of " +
                    AbstractService.class.getName());

            System.exit(1);
        }

        String serviceChannelStack=res.getString(SERVICE_CHANNEL_STACK);
        final JChannel svcChannel=new JChannel(serviceChannelStack);
        svcChannel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);

        String clientChannelStack=res.getString(CLIENT_CHANNEL_STACK);

        final JChannel clientChannel=new JChannel(clientChannelStack);

        String svcGroup=res.getString(SERVICE_GROUP_NAME);
        String clientGroup=res.getString(CLIENT_GROUP_NAME);

        svcChannel.connect(svcGroup);
        clientChannel.connect(clientGroup);

        java.lang.reflect.Constructor serviceConstructor=
                serviceClass.getConstructor(new Class[]{Channel.class, Channel.class});

        final AbstractService service=(AbstractService)
                serviceConstructor.newInstance(new Object[]{svcChannel, clientChannel});

        service.start();

        Runnable shutdownHook=new Runnable() {
            public void run() {
                System.out.println("Shutting down service " + service.getName());

                service.stop();
                svcChannel.close();
                clientChannel.close();

                System.out.println("Done.");

            }
        };

        Thread shutdownThread=new Thread(shutdownHook, service.getName() +
                " shutdown hook [" + service.getAddress() + ']');
        shutdownThread.setDaemon(true);

        Runtime.getRuntime().addShutdownHook(shutdownThread);

        System.out.println("Service '" + service.getName() + "' is up'n'running");
    }

    /**
     * Main entry to run this class.
     */
    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            printUsage();
            System.exit(0);
        }

        String resourceName=null;

        for(int i=0; i < args.length; i++) {
            if(HELP_SWITCH.equals(args[i])) {
                printUsage();
                System.exit(0);
            }
            else
                if(RESOURCE_SWITCH.equals(args[i])) {
                    resourceName=args[++i];
                }
        }

        if(resourceName == null) {
            printUsage();
            System.exit(0);
        }

        ResourceBundle res=ResourceBundle.getBundle(resourceName);

        startService(res);
    }

    /**
     * Print usage of this class
     */
    private static void printUsage() {
        System.out.println();
        System.out.println("Usage: java org.jgroups.service.ServiceRunner -res <service_desc_res>");

        System.out.println("<service_desc_res> -\tservice description resource,");
        System.out.println("\t\t\tstandard properties file containing information");
        System.out.println("\t\t\tabout service to run.");
    }
}
