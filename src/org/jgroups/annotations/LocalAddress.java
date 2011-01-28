package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This is an assertion, checked at startup time. It ensures that the field this annotation is on is (1) an InetAddress
 * and (2) a valid address on a local network interface
 * @author Bela Ban
 * @since 2.11.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LocalAddress {
}

