package org.jgroups.annotations;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Elements annotated with this annotation are experimental and may get removed from the distribution at any time
 * @author Bela Ban
 * @version $Id: Experimental.java,v 1.2 2008/04/08 14:41:21 belaban Exp $
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PACKAGE})
@Retention(RetentionPolicy.SOURCE)
public @interface Experimental {
    String comment() default "";
}