package org.jgroups.annotations;

import java.lang.annotation.*;

/**
 * 
 * Represents an array of deprecated Protocol properties 
 * 
 *
 * @author Vladimir Blagojevic
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE})
public @interface DeprecatedProperty {

    String [] names() default"";    
}
