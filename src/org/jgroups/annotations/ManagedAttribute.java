package org.jgroups.annotations;

/**
 * Indicates that a public method or a field (any visibility) in 
 * an MBean class defines an MBean attribute. This annotation can 
 * be applied to either a field or a public setter and/or getter 
 * method of a public class that is itself is optionally annotated 
 * with an @MBean annotation, or inherits such an annotation from 
 * a superclass.
 * 
 * @author Chris Mills
 * @version $Id: ManagedAttribute.java,v 1.3 2008/03/11 02:26:20 vlada Exp $
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface ManagedAttribute {
    String description() default "";

    boolean readable() default true;

    boolean writable() default false;
}
