package org.jgroups.annotations;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Elements annotated with this annotation are unsupported and may get removed from the distribution at any time
 * @author Bela Ban
 * @version $Id: Unsupported.java,v 1.3 2008/10/21 12:10:32 vlada Exp $
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PACKAGE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Unsupported {
    String comment() default "";
}
