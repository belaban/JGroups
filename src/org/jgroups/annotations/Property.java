package org.jgroups.annotations;

import org.jgroups.conf.AttributeType;
import org.jgroups.conf.PropertyConverter;
import org.jgroups.conf.PropertyConverters;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Represents a Protocol property assigned from corresponding field in JGroups
 * properties file.
 * 
 * <p>
 * Since all protocol properties are read as String instances from properties
 * file properties need to be converted to an appropriate field type of a
 * matching Protocol instance. JGroups supplies set of converters in
 * {@link PropertyConverters} class.
 * 
 * <p>
 * Third parties can provide their own converters if such need arises by
 * implementing {@link PropertyConverter} interface and by specifying that
 * converter as converter on a specific Property annotation of a field or a
 * method instance.
 * 
 * <p>
 * Property annotation can decorate either a field or a method of a Property
 * class. If a method is decorated with Property annotation it is assumed that
 * such a method is a setter with only one parameter type that a specified
 * converter can convert from a String to an actual type.
 * 
 * 
 * @author Vladimir Blagojevic
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface Property {

    String name() default "";

    String description() default "";

    String deprecatedMessage() default "";

    Class<?> converter() default PropertyConverters.Default.class;    
    
    String dependsUpon() default "";

    String[] systemProperty() default {};

    /**
     * Global.NON_LOOPBACK_ADDRESS means pick any valid non-loopback IPv4 address
     */
    String defaultValueIPv4() default "" ;

    /**
     * Global.NON_LOOPBACK_ADDRESS means pick any valid non-loopback IPv6 address
     */
    String defaultValueIPv6() default "" ;

    /** Expose this property also as a managed attribute */
    boolean exposeAsManagedAttribute() default true;

    /* Should this managed attribute be writeable ? If set to true, automatically sets exposeAsManagedAttribute to true */
    boolean writable() default true;

    /** Defines the type, used for pretty printing */
    AttributeType type() default AttributeType.UNDEFINED;

    /** Only used if type is TIME */
    TimeUnit unit() default TimeUnit.MILLISECONDS;
}


