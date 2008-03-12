package org.jgroups.annotations;

import java.lang.annotation.*;

/**
 * Indicates that a method in an MBean class defines an MBean 
 * operation. @ManagedOperation annotation can be applied to a 
 * public method of a public class that is itself optionally 
 * annotated with an @MBean annotation, or inherits such an 
 * annotation from a superclass.
 * 
 * @author Chris Mills
 * @version $Id: ManagedOperation.java,v 1.4 2008/03/12 00:26:42 vlada Exp $
 */


@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD })
public @interface ManagedOperation {
    String description() default "";
}
