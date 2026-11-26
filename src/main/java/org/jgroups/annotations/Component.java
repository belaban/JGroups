package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Tags components inside of protocols. Used to generate the schema, configure a component via XML and expose
 * attributes/operations via JMX or probe. See https://issues.redhat.com/browse/JGRP-2567 for details
 * @author Bela Ban
 * @since 5.2
 */
@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface Component {

    String name() default "";

    String description() default "";

    String deprecatedMessage() default "";
}


