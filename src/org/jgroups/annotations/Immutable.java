package org.jgroups.annotations;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Adopted from Java Concurrency in Practice. This annotation defines an immutable class, ie. a class whose
 * instances cannot be modified after creation
 * @author Bela Ban
 * @version $Id: Immutable.java,v 1.1 2007/01/17 14:49:12 belaban Exp $
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Immutable {
}
