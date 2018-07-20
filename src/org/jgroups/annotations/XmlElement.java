package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to generate an element in the schema created by {@link org.jgroups.util.XMLSchemaGenerator}. Example: <p>
 * <pre>
 * {@literal @}XmlElement(name="fork-stacks" type="ForkStacksType")
 * </pre>
 * This results in the following schema fragment:
 * <pre>
 *     &lt;xs:element name="fork-stacks" type="ForkStacksType"/&gt;
 * </pre>
 * @author Bela Ban
 * @since  3.5
 */
@Retention(value=RetentionPolicy.RUNTIME)
@Target(value=ElementType.TYPE)
public @interface XmlElement {
    String name();
    String type();
}
