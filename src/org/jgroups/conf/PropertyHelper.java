package org.jgroups.conf ;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.Configurator;

    /*
     * A class of static methods for performing commonly used functions with @Property annotations.
     */
    public class PropertyHelper {
    	
        protected static final Log log=LogFactory.getLog(PropertyHelper.class);
    	
    	public static String getPropertyName(Field field, Properties props) throws IllegalArgumentException {
    		if (field == null) {
    			throw new IllegalArgumentException("Cannot get property name: field is null") ;
    		}
    		if (props == null) {
    			throw new IllegalArgumentException("Cannot get property name: properties map is null") ;
    		}    		
    		Property annotation=field.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot get property name for field " + 
    					field.getName() + " which is not annotated with @Property") ;
    		}
    		String propertyName=field.getName();
    		if(props.containsKey(annotation.name())) {
    			propertyName=annotation.name();
    			boolean isDeprecated=annotation.deprecatedMessage().length() > 0;
    			if(isDeprecated && log.isWarnEnabled()) {
    				log.warn(annotation.deprecatedMessage());
    			}
    		}
    		if (log.isDebugEnabled()) {
    			log.debug("Property name=" + propertyName + " (annotation name=" + 
    					annotation.name() + ", field name=" + field.getName() + ")") ;
    		}
    		return propertyName ;
    	}
    	
    	public static String getPropertyName(Method method) throws IllegalArgumentException {
    		if (method == null) {
    			throw new IllegalArgumentException("Cannot get property name: field is null") ;
    		}
    		Property annotation=method.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot get property name for method " + 
    					method.getName() + " which is not annotated with @Property") ;
    		}    		
    		String propertyName=annotation.name().length() > 0? annotation.name() : method.getName().substring(3);
    		propertyName=Configurator.renameFromJavaCodingConvention(propertyName);
    		if (log.isDebugEnabled()) {
    			log.debug("Property name=" + propertyName + " (annotation name=" + 
    					annotation.name() + ", method name=" + method.getName() + ")") ;
    		}
    		return propertyName ;
    	}
    	
    	public static Object getConvertedValue(Object obj, Field field, Properties props, String prop) 
    			throws IllegalArgumentException, Exception {
    		if (obj == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Object is null") ;
    		}
    		if (field == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Field is null") ;
    		}
    		if (props == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Properties is null") ;
    		}
//    		if (prop == null) {
//    			throw new IllegalArgumentException("Cannot get converted value: property string value is null") ;
//    		}
    		Property annotation=field.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot get property name for field " + 
    					field.getName() + " which is not annotated with @Property") ;
    		}
			String propertyName = getPropertyName(field, props) ;
			String name = obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();

    		PropertyConverter propertyConverter=(PropertyConverter)annotation.converter().newInstance();
    		if(propertyConverter == null) {    				
    			throw new Exception("Could not find property converter for field " + propertyName
    					+ " in " + name);
    		}
    		Object converted = null ;
			try {
				converted=propertyConverter.convert(obj, field.getType(), props, prop);
			}
			catch(Exception e) {
				throw new Exception("Conversion of " + propertyName + " in " + name + 
						" with original property value " + prop  + " failed. Exception is " +e, e);
			}
    		if (log.isDebugEnabled()) {
    			if (converted != null)
    				log.debug("Converted value=" + converted.toString() + " (propertyName="+ propertyName + ", propertyValue=" + prop + ")") ;
    			else
    				log.debug("Converted value=null (propertyName="+ propertyName + ", propertyValue=" + prop + ")") ;
    		}
			return converted ;
    	}

    	public static Object getConvertedValue(Object obj, Method method, Properties props, String prop) 
    	throws IllegalArgumentException, Exception {
    		if (obj == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Object is null") ;
    		}
    		if (method == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Method is null") ;
    		}
    		if (!Configurator.isSetPropertyMethod(method)) {
    			throw new IllegalArgumentException("Cannot get converted value: Method is not set property method") ;
    		}
    		if (props == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Properties is null") ;
    		}
//    		if (prop == null) {
//    			throw new IllegalArgumentException("Cannot get converted value: property string value is null") ;
//    		}
    		Property annotation=method.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot get property name for method " + 
    					method.getName() + " which is not annotated with @Property") ;
    		}
    		String propertyName = getPropertyName(method) ;
    		String name = obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();
    		PropertyConverter propertyConverter=(PropertyConverter)annotation.converter().newInstance();
    		if(propertyConverter == null) {    				
    			throw new Exception("Could not find property converter for method " + propertyName
    					+ " in " + name);
    		}
    		Object converted = null ;
    		try {
    			converted=propertyConverter.convert(obj, method.getParameterTypes()[0], props, prop);
    		}
    		catch(Exception e) {
				throw new Exception("Conversion of " + propertyName + " in " + name + 
						" with original property value " + prop  + " failed. Exception is " +e, e);
    		}	
    		if (log.isDebugEnabled()) {
    			if (converted != null)
    				log.debug("Converted value=" + converted.toString() + " (propertyName="+ propertyName + ", propertyValue=" + prop + ")") ;
    			else
    				log.debug("Converted value=null (propertyName="+ propertyName + ", propertyValue=" + prop + ")") ;
    		}
    		return converted ;
    	}
    	
        public static boolean usesDefaultConverter(Field field) throws IllegalArgumentException {
    		if (field == null) {
    			throw new IllegalArgumentException("Cannot check converter: Field is null") ;
    		}
    		Property annotation=field.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot check converter for Field " + 
    					field.getName() + " which is not annotated with @Property") ;
    		}
        	return annotation.converter().equals(PropertyConverters.Default.class) ;
        }
        
    }
