package org.jgroups.annotations;

import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Copyright (c) 2005 Brian Goetz and Tim Peierls
 * Released under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/2.5)
 * Official home: http://www.jcip.net
 * 
 * Adopted from Java Concurrency in Practice. This annotation defines an immutable class, ie. a class whose
 * instances cannot be modified after creation
 * @author Bela Ban
 * @version $Id: Immutable.java,v 1.2 2007/02/27 14:49:40 belaban Exp $
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Immutable {
}
