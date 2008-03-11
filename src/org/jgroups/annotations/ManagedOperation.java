package org.jgroups.annotations;

/**
 * Indicates that a method in an MBean class defines an MBean 
 * operation. @ManagedOperation annotation can be applied to a 
 * public method of a public class that is itself optionally 
 * annotated with an @MBean annotation, or inherits such an 
 * annotation from a superclass.
 * 
 * @author Chris Mills
 * @version $Id: ManagedOperation.java,v 1.3 2008/03/11 02:26:20 vlada Exp $
 */

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD })
public @interface ManagedOperation {
    String description() default "";
}
