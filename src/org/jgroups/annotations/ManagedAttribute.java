package org.jgroups.annotations;

/**
 * Indicates that a public method or a field with any visibility in an MBean class 
 * defines an MBean attribute. This annotation can be applied to either a field or 
 * a public method of a public class that is itself annotated with an @MBean 
 * annotation, or inherits such an annotation from a superclass.
 * 
 * @author Chris Mills
 * @version $Id: ManagedAttribute.java,v 1.2 2008/03/06 00:50:16 vlada Exp $
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface ManagedAttribute {
    String description() default "";

    boolean readable() default true;

    boolean writable() default false;
}
