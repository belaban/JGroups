package org.jgroups.annotations;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to add attributes to the schema created by {@link org.jgroups.util.XMLSchemaGenerator}. Example: <br/>
 * <pre>
 * {@literal @}XmlAttribute(attrs={"auth_value", "demo_token})
 * </pre>
 * This results in the following schema fragment:
 * <pre>
 *     &lt;xs:attribute name="auth_value" type="xs:string"/&gt;
       &lt;xs:attribute name="demo_token" type="xs:string"/&gt;
 * </pre>
 * @author Bela Ban
 * @since  3.5
 */
@Retention(value=RetentionPolicy.RUNTIME)
@Target(value=ElementType.TYPE)
public @interface XmlAttribute {
    String[] attrs() default {};
}
