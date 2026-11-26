package org.jgroups.conf;

/**
 * Defines the type of an attribute exposed by {@link org.jgroups.annotations.ManagedAttribute} or
 * {@link org.jgroups.annotations.Property}. This is used to format the output in a more legible way.<br/>
 * JIRA: https://issues.redhat.com/browse/JGRP-2457.
 * @author Bela Ban
 * @since  5.0.0
 */
public enum AttributeType {
    UNDEFINED,
    TIME,
    BYTES,
    SCALAR
}
