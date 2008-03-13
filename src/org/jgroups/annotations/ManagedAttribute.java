package org.jgroups.annotations;
import java.lang.annotation.*;

/**
 * Indicates that a public method or a field (any visibility) in 
 * an MBean class defines an MBean attribute. This annotation can 
 * be applied to either a field or a public setter and/or getter 
 * method of a public class that is itself is optionally annotated 
 * with an @MBean annotation, or inherits such an annotation from 
 * a superclass.
 * 
 * @author Chris Mills
 * @version $Id: ManagedAttribute.java,v 1.6 2008/03/13 02:00:23 vlada Exp $
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface ManagedAttribute {
    String description() default "";

    String name() default "";
    
    boolean writable() default false;
}
