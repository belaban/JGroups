package org.jgroups.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to include other schemas by {@link org.jgroups.util.XMLSchemaGenerator}. Example:
 * <pre>
 *     {@literal @}XmlInclude(schema="relay.xsd",type=Type.IMPORT,namespace="urn:jgroups:relay:1.0",alias="relay")
 * </pre>
 * results in the following include in the schema element:
 * <pre>
 *     &lt;xs:schema... xmlns:relay="urn:jgroups:relay:1.0" /&gt;
 *     ...
 *     &lt;xs:import schemaLocation="fork-stacks.xsd" namespace="urn:jgroups:relay:1.0" /&gt;
 * </pre>
 * @author Bela Ban
 * @since  3.5
 */
@Retention(value=RetentionPolicy.RUNTIME)
@Target(value=ElementType.TYPE)
public @interface XmlInclude {
    Type type() default Type.INCLUDE;
    String[] schema() default {};
    String namespace() default ""; // only used if type == IMPORT
    String alias() default "";     // only used if type == IMPORT

    enum Type {INCLUDE, IMPORT};
}
