package org.jgroups.annotations;

import java.lang.annotation.*;

import org.jgroups.conf.PropertyConverters;

/**
 * 
 * @version $Id: Property.java,v 1.1 2008/05/07 08:35:17 vlada Exp $
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.METHOD, ElementType.FIELD })
public @interface Property {
    Class converter() default PropertyConverters.Default.class;     

    String description() default "";

    String name() default "";

    String deprecatedMessage() default "";
}
