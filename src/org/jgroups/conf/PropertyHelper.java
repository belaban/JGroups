package org.jgroups.conf ;

import org.jgroups.annotations.Property;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

    /*
     * A class of static methods for performing commonly used functions with @Property annotations.
     */
    public final class PropertyHelper {
    	
        protected static final Log log=LogFactory.getLog(PropertyHelper.class);
    	
    	private PropertyHelper() {
    		throw new InstantiationError( "Must not instantiate this class" );
    	}

    	public static String getPropertyName(Field field, Map<String,String> props) throws IllegalArgumentException {
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
    			boolean isDeprecated=!annotation.deprecatedMessage().isEmpty();
    			if(isDeprecated)
                    log.warn(Util.getMessage("Deprecated"), propertyName, annotation.deprecatedMessage());
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
    		String propertyName=!annotation.name().isEmpty()? annotation.name() : method.getName();
    		propertyName=Util.methodNameToAttributeName(propertyName);
    		return propertyName ;
    	}
    	
    	public static Object getConvertedValue(Object obj, Field field, Map<String, String> props, String prop, boolean check_scope) throws Exception {
    		if (obj == null)
    			throw new IllegalArgumentException("Cannot get converted value: Object is null") ;
    		if (field == null)
    			throw new IllegalArgumentException("Cannot get converted value: Field is null") ;
    		if (props == null)
    			throw new IllegalArgumentException("Cannot get converted value: Properties is null") ;

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
                String tmp=obj instanceof Protocol? ((Protocol)obj).getName() + "." + propertyName : propertyName;
				converted=propertyConverter.convert(obj, field.getType(), tmp, prop, check_scope);
			}
			catch(Exception e) {
				throw new Exception("Conversion of " + propertyName + " in " + name + 
						" with original property value " + prop  + " failed", e);
			}
			return converted ;
    	}


        public static Object getConvertedValue(Object obj, Field field, String value, boolean check_scope) throws Exception {
            if(obj == null)
                throw new IllegalArgumentException("Cannot get converted value: Object is null");
            if(field == null)
                throw new IllegalArgumentException("Cannot get converted value: Field is null");

            Property annotation=field.getAnnotation(Property.class);
            if(annotation == null) {
                throw new IllegalArgumentException("Cannot get property name for field " +
                        field.getName() + " which is not annotated with @Property");
            }
            String propertyName=field.getName();
            String name=obj instanceof Protocol? ((Protocol)obj).getName() : obj.getClass().getName();

            PropertyConverter propertyConverter=(PropertyConverter)annotation.converter().newInstance();
            if(propertyConverter == null) {
                throw new Exception("Could not find property converter for field " + propertyName
                        + " in " + name);
            }
            Object converted=null;
            try {
                String tmp=obj instanceof Protocol? ((Protocol)obj).getName() + "." + propertyName : propertyName;
                converted=propertyConverter.convert(obj, field.getType(), tmp, value, check_scope);
            }
            catch(Exception e) {
                throw new Exception("Conversion of " + propertyName + " in " + name +
                        " with original property value " + value + " failed", e);
            }
            return converted;
        }


    	public static Object getConvertedValue(Object obj, Method method, Map<String, String> props, String prop, boolean check_scope) throws Exception {
    		if (obj == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Object is null") ;
    		}
    		if (method == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Method is null") ;
    		}
    		if (!Configurator.isSetPropertyMethod(method, obj.getClass())) {
    			throw new IllegalArgumentException("Cannot get converted value: Method is not set property method") ;
    		}
    		if (props == null) {
    			throw new IllegalArgumentException("Cannot get converted value: Properties is null") ;
    		}
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
                String tmp=obj instanceof Protocol? ((Protocol)obj).getName() + "." + propertyName : propertyName;
    			converted=propertyConverter.convert(obj, method.getParameterTypes()[0], tmp, prop, check_scope);
    		}
    		catch(Exception e) {
				throw new Exception("Conversion of " + propertyName + " in " + name + 
						" with original property value " + prop  + " failed. Exception is " +e, e);
    		}	
    		return converted ;
    	}
    	
        public static boolean usesDefaultConverter(Field field) throws IllegalArgumentException {
    		if (field == null) {
    			throw new IllegalArgumentException("Cannot check converter: field is null") ;
    		}
    		Property annotation=field.getAnnotation(Property.class);
    		if (annotation == null) {
    			throw new IllegalArgumentException("Cannot check converter for field " +
    					field.getName() + " which is not annotated with @Property") ;
    		}
        	return annotation.converter().equals(PropertyConverters.Default.class) ;
        }
        
    }
