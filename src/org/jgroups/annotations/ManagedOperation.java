package org.jgroups.annotations;

/**
 * User: Chris Mills
 * Date: Aug 14, 2007
 * Time: 2:46:34 PM
 */

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})

public @interface ManagedOperation {
    String description() default "";
}
