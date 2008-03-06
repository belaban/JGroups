package org.jgroups.annotations;

/**
 * User: Chris Mills
 * Date: Aug 13, 2007
 * Time: 7:27:17 PM
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited

public @interface MBean {
    String objectName() default "";    
    String description() default "";
}
