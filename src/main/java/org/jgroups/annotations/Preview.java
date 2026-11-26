package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Any element annotated with this annotation is made available as a preview feature and is not yet production-grade.
 *
 * @author Radoslav Husar
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PACKAGE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Preview {
    String comment() default "";
}
