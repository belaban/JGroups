package org.jgroups.annotations;

import org.jgroups.conf.AttributeType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Indicates that a public method or a field (any visibility) in 
 * an MBean class defines an MBean attribute. This annotation can 
 * be applied to either a field or a public setter and/or getter 
 * method of a public class that is itself is optionally annotated 
 * with an @MBean annotation, or inherits such an annotation from 
 * a superclass.
 * 
 * @author Chris Mills
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface ManagedAttribute {
    String description() default "";

    String name() default "";
    
    boolean writable() default false;

    /* Defines the type, used for pretty printing */
    AttributeType type() default AttributeType.UNDEFINED;

    /** Only used if type is TIME */
    TimeUnit unit() default TimeUnit.MILLISECONDS;
}
