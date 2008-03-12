package org.jgroups.annotations;
import java.lang.annotation.*;
/**
 * Optional annotation that exposes all public methods in the class 
 * hierarchy (excluding Object) as MBean operations.
 * <p>
 * 
 * If a more fine grained MBean attribute and operation exposure is needed 
 * do not use @MBean annotation but annotate fields and public methods directly 
 * using @ManagedOperation and @ManagedAttribute annotations.
 * 
 * 
 * @author Chris Mills
 * @version $Id: MBean.java,v 1.5 2008/03/12 04:48:15 vlada Exp $
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE })
@Inherited
public @interface MBean {
    String objectName() default "";

    String description() default "";
}
