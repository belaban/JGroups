package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Adopted from Java Concurrency in Practice. This annotation defines the monitor that protects the variable
 * annotated by @GuardedBy, e.g. @GuardedBy("lock") or @GuardedBy("this")
 * @author Bela Ban
 * @version $Id: GuardedBy.java,v 1.1 2007/01/08 10:57:20 belaban Exp $
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface GuardedBy {
    String value();
}
