package org.jgroups.conf;

import java.util.*;

/**
 * A simple replacement for a W3C DOM node.
 * @author Bela Ban
 * @since  5.1.4
 */
public class XmlNode {
    protected final String       name;
    protected Map<String,String> attributes;
    protected List<XmlNode>      children;

    public XmlNode(String name) {
        this.name=Objects.requireNonNull(name);
    }

    public String             getName()       {return name;}
    public Map<String,String> getAttributes() {return attributes;}
    public List<XmlNode>      getChildren()   {return children;}

    public XmlNode setAttributes(Map<String,String> attrs) {
        this.attributes=attrs;
        return this;
    }

    public XmlNode addAttribute(String attr_name, String val) {
        if(attributes == null)
            attributes=new HashMap<>();
        attributes.put(attr_name, val);
        return this;
    }

    public XmlNode addChild(XmlNode n) {
        if(children == null)
            children=new ArrayList<>();
        children.add(n);
        return this;
    }

    @Override
    public String toString() {
        return print(0);
    }

    protected String print(int indent) {
        StringBuilder sb=new StringBuilder(String.format("%s%s", " ".repeat(indent), name));
        if(attributes != null)
            sb.append(" ").append(attributes);
        sb.append("\n");
        if(children != null) {
            for(XmlNode child: children)
                sb.append(child.print(indent+2));
        }
        return sb.toString();
    }
}
