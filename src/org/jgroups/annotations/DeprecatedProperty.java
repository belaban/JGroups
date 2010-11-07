package org.jgroups.annotations;

import java.lang.annotation.*;

/**
 * 
 * Represents an array of deprecated Protocol properties 
 * 
 *
 * @author Vladimir Blagojevic
 * @version $Id: DeprecatedProperty.java,v 1.1 2008/05/29 13:53:04 vlada Exp $
 */

@Retention(RetentionPolicy.RUNTIME)
@Target( { ElementType.TYPE})
public @interface DeprecatedProperty {

    String [] names() default"";    
}
