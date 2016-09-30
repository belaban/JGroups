package org.jgroups.logging;

import org.jgroups.Global;

import java.util.Locale;


/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @author Bela Ban
 * @since 4.0
 */
public final class LogFactory {
    private static final Locale      LOCALE = new Locale(
            System.getProperty("user.language"),
            System.getProperty("user.country"));

    public static final boolean       IS_SLF4J_AVAILABLE;   // slf4j is the default
    public static final boolean       IS_LOG4J2_AVAILABLE;
    protected static boolean          use_jdk_logger;
    protected static CustomLogFactory custom_log_factory=null;

	private LogFactory() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    static {
        use_jdk_logger=isPropertySet(Global.USE_JDK_LOGGER);
        IS_LOG4J2_AVAILABLE=isAvailable("org.apache.logging.log4j.core.Logger");
        IS_SLF4J_AVAILABLE=isAvailable("org.slf4j.Logger");
    }

    public static CustomLogFactory getCustomLogFactory()                         {return custom_log_factory;}
    public static void             setCustomLogFactory(CustomLogFactory factory) {custom_log_factory=factory;}
    public static boolean          useJdkLogger()                                {return use_jdk_logger;}
    public static void             useJdkLogger(boolean flag)                    {use_jdk_logger=flag;}

    public static String loggerType() {
        if(use_jdk_logger)      return "jdk";
        if(IS_SLF4J_AVAILABLE)  return "slf4j";
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

        if (IS_SLF4J_AVAILABLE)
            return new Slf4jLogImpl(LOCALE, clazz);

        if(IS_LOG4J2_AVAILABLE)
            return new Log4J2LogImpl(clazz);

        return new JDKLogImpl(clazz);
    }

    public static Log getLog(String category) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(category);

        if(use_jdk_logger)
            return new JDKLogImpl(category);

        if (IS_SLF4J_AVAILABLE)
            return new Slf4jLogImpl(LOCALE, category);

        if(IS_LOG4J2_AVAILABLE)
            return new Log4J2LogImpl(category);

        return new JDKLogImpl(category);
    }
}
