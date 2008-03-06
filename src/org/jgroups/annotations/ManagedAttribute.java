package org.jgroups.annotations;

/**
 * User: Chris Mills
 * Date: Aug 13, 2007
 * Time: 7:27:24 PM
 */
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})

public @interface ManagedAttribute {
    String description() default "";
    boolean readable() default true;
    boolean writable() default false;
}
