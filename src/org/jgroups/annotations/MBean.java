package org.jgroups.annotations;

/**
 * Optional annotation that exposes all public methods in the class 
 * hierarchy (excluding Object) as MBean operations.
 * <p>
 * 
 * Furthermore, class fields (any visibility) along with public getter 
 * and setter methods in a class hierarchy of a class annotated with @MBean 
 * annotation can be annotated with @ManagedAttribute to indicate that 
 * they define MBean attributes. If such methods and fields are discovered 
 * they will be exposed as MBean attributes.
 * <p> 
 * 
 * If a more fine grained MBean attribute and operation exposure is needed 
 * do not use @MBean annotation but annotate fields and public methods directly 
 * using @ManagedOPeration and @ManagedAttribute annotations.
 *  
 * 
 * 
 * @author Chris Mills
 * @version $Id: MBean.java,v 1.3 2008/03/11 02:26:20 vlada Exp $
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE })
@Inherited
public @interface MBean {
    String objectName() default "";

    String description() default "";
}
