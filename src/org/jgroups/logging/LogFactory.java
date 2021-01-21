package org.jgroups.logging;

import org.jgroups.Global;
import org.jgroups.util.Util;

import java.lang.reflect.Constructor;


/**
 * Factory that creates {@link Log} instances.
 *
 * @author Manik Surtani
 * @author Bela Ban
 * @since 4.0
 */
public final class LogFactory {


    public static final boolean                 IS_SLF4J_AVAILABLE;
    public static final boolean                 IS_LOG4J2_AVAILABLE;  // log4j2 is the default logger
    protected static boolean                    use_jdk_logger;
    protected static CustomLogFactory           custom_log_factory=null;
    protected static Constructor<? extends Log> ctor_class=null;

	private LogFactory() {
		throw new InstantiationError( "Must not instantiate this class" );
	}

    static {
        use_jdk_logger=isPropertySet(Global.USE_JDK_LOGGER);

        String classname=Util.getProperty(Global.LOG_CLASS);
        if(classname != null) {
            try {
                ctor_class=findConstructor(classname, Class.class);
            }
            catch(Exception e) {
                throw new IllegalArgumentException(String.format("failed loading logger %s", classname), e);
            }
        }

        if(use_jdk_logger || ctor_class != null)
            IS_LOG4J2_AVAILABLE=IS_SLF4J_AVAILABLE=false;
        else {
            IS_LOG4J2_AVAILABLE=isAvailable("org.apache.logging.log4j.core.Logger");
            IS_SLF4J_AVAILABLE=!IS_LOG4J2_AVAILABLE && isAvailable("org.slf4j.Logger");
        }
    }

    public static CustomLogFactory getCustomLogFactory()                         {return custom_log_factory;}
    public static void             setCustomLogFactory(CustomLogFactory factory) {custom_log_factory=factory;}
    public static boolean          useJdkLogger()                                {return use_jdk_logger;}
    public static void             useJdkLogger(boolean flag)                    {use_jdk_logger=flag;}

    public static Log getLog(Class<?> clazz) {
        if(custom_log_factory != null)
            return custom_log_factory.getLog(clazz);
        if(ctor_class != null) {
            try {
                return ctor_class.newInstance(clazz);
            }
            catch(Throwable t) {
                throw new RuntimeException(t);
            }
        }
        if(use_jdk_logger)
            return new JDKLogImpl(clazz);
        if(IS_LOG4J2_AVAILABLE)
            return new Log4J2LogImpl(clazz);
        if(IS_SLF4J_AVAILABLE)
            return new Slf4jLogImpl(clazz);
        return new JDKLogImpl(clazz);
    }

    public static String loggerType() {
        if(ctor_class != null)
            return ctor_class.getDeclaringClass().getSimpleName();
        if(use_jdk_logger)      return "jdk";
        if(IS_LOG4J2_AVAILABLE) return "log4j2";
        if(IS_SLF4J_AVAILABLE)  return "slf4j";
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


    protected static Constructor<? extends Log> findConstructor(String classname, Class<?> arg) throws Exception {
        Class<?> clazz=Util.loadClass(classname, (Class<?>)null);
        return (Constructor<? extends Log>)clazz.getDeclaredConstructor(arg);
    }
}
