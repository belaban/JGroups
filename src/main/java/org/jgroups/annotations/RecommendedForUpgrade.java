package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Protocols annotated with this annotation should upgrade to a newer version (e.g. UNICAST -> UNICAST3)
 * @author Bela Ban
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RecommendedForUpgrade {
}
