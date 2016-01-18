package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Elements annotated with this annotation are experimental and may get removed from the distribution at any time
 * @author Bela Ban
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PACKAGE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Experimental {
    String comment() default "";
}