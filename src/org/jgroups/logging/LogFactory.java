package org.jgroups.logging;

import org.jgroups.Global;


/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @author Bela Ban
 * @since 4.0
 */
public class LogFactory {
    public static final boolean IS_LOG4J_AVAILABLE;
    protected static boolean USE_JDK_LOGGER;

    private static CustomLogFactory custom_log_factory;

    static {
        USE_JDK_LOGGER=isPropertySet(Global.USE_JDK_LOGGER);
        boolean available;
        try {
            Class.forName("org.apache.log4j.Logger");
            available=true;
        }
        catch(ClassNotFoundException cnfe) {
            available=false;
        }
        IS_LOG4J_AVAILABLE=available;
    }

    public static CustomLogFactory getCustomLogFactory()                         {return custom_log_factory;}
    public static void             setCustomLogFactory(CustomLogFactory factory) {custom_log_factory=factory;}
    public static boolean          useJdkLogger()                                {return USE_JDK_LOGGER;}
    public static void             useJdkLogger(boolean flag)                    {USE_JDK_LOGGER=flag;}

    public static Log getLog(Class clazz) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(clazz);

        // this call is not executed frequently, so we don't need to move the check for USE_JDK_LOGGER to class init time
        final boolean use_jdk_logger=Boolean.parseBoolean(System.getProperty(Global.USE_JDK_LOGGER));
        if(IS_LOG4J_AVAILABLE && !use_jdk_logger) {
            return new Log4JLogImpl(clazz);
        }
        else {
            return new JDKLogImpl(clazz);
        }
    }

    public static Log getLog(String category) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(category);
        
        final boolean use_jdk_logger=Boolean.parseBoolean(System.getProperty(Global.USE_JDK_LOGGER));
        if(IS_LOG4J_AVAILABLE && !use_jdk_logger) {
            return new Log4JLogImpl(category);
        }
        else {
            return new JDKLogImpl(category);
        }
    }

    protected static boolean isPropertySet(String property_name) {
        try {
            return Boolean.parseBoolean(System.getProperty(property_name));
        }
        catch(Throwable t) {
            return false;
        }
    }
}
