package org.jgroups.annotations;

/**
 * Indicates that the annotated class is a Standard MBean. The class must be public. 
 * Public methods within the class can be annotated with @ManagedOperation to indicate 
 * that they are MBean operations. Class fields and public getter and setter methods 
 * within the class can be annotated with @ManagedAttribute to indicate that they 
 * define MBean attributes.
 * 
 * @author Chris Mills
 * @version $Id: MBean.java,v 1.2 2008/03/06 00:50:16 vlada Exp $
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE })
@Inherited
public @interface MBean {
    String objectName() default "";

    String description() default "";
}
