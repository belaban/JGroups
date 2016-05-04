package org.jgroups.logging;

import org.jgroups.Global;


/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @author Bela Ban
 * @since 4.0
 */
public final class LogFactory {
    public static final boolean       IS_LOG4J2_AVAILABLE; // log4j2 is the default
    protected static boolean          use_jdk_logger;
    protected static CustomLogFactory custom_log_factory=null;

	private LogFactory() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    static {
        use_jdk_logger=isPropertySet(Global.USE_JDK_LOGGER);
        IS_LOG4J2_AVAILABLE=isAvailable("org.apache.logging.log4j.core.Logger");
    }

    public static CustomLogFactory getCustomLogFactory()                         {return custom_log_factory;}
    public static void             setCustomLogFactory(CustomLogFactory factory) {custom_log_factory=factory;}
    public static boolean          useJdkLogger()                                {return use_jdk_logger;}
    public static void             useJdkLogger(boolean flag)                    {use_jdk_logger=flag;}

    public static String loggerType() {
        if(use_jdk_logger)      return "jdk";
        if(IS_LOG4J2_AVAILABLE) return "log4j2";
        return "jdk";
    }

    protected static boolean isAvailable(String classname) {
        try {
            return Class.forName(classname) != null;
        }
        catch(ClassNotFoundException cnfe) {
            return false;
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

    public static Log getLog(Class clazz) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(clazz);

        if(use_jdk_logger)
            return new JDKLogImpl(clazz);

        if(IS_LOG4J2_AVAILABLE)
            return new Log4J2LogImpl(clazz);

        return new JDKLogImpl(clazz);
    }

    public static Log getLog(String category) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(category);

        if(use_jdk_logger)
            return new JDKLogImpl(category);

        if(IS_LOG4J2_AVAILABLE)
            return new Log4J2LogImpl(category);

        return new JDKLogImpl(category);
    }
}
