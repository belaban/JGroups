
package org.jgroups.conf;


import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.jgroups.util.Util.readTillMatchingCharacter;

/**
 * Uses XML to configure a protocol stack
 * @author Vladimir Blagojevic
 * @author Bela Ban
 */
public class XmlConfigurator implements ProtocolStackConfigurator {
    private final List<ProtocolConfiguration> configuration=new ArrayList<>();
    protected static final Log                log=LogFactory.getLog(XmlConfigurator.class);

    protected enum ElementType {
        START, COMPLETE, END, COMMENT, UNDEFINED
    }
    
    protected XmlConfigurator(List<ProtocolConfiguration> protocols) {
        configuration.addAll(protocols);
    }

    public static XmlConfigurator getInstance(InputStream stream) throws java.io.IOException {
        return parse(stream);
    }

    /**
     * 
     * @param convert If false: print old plain output, else print new XML format
     * @return String with protocol stack in specified format
     */
    public String getProtocolStackString(boolean convert) {
        StringBuilder buf=new StringBuilder();
        Iterator<ProtocolConfiguration> it=configuration.iterator();
        if(convert)
            buf.append("<config>\n");
        while(it.hasNext()) {
            ProtocolConfiguration d=it.next();
            if(convert)
                buf.append("    <");
            buf.append(d.getProtocolString(convert));
            if(convert)
                buf.append("/>");
            if(it.hasNext()) {
                if(convert)
                    buf.append('\n');
                else
                    buf.append(':');
            }
        }
        if(convert)
            buf.append("\n</config>");
        return buf.toString();
    }

    public String getProtocolStackString() {
        return getProtocolStackString(false);
    }

    public List<ProtocolConfiguration> getProtocolStack() {
        return configuration;
    }



    protected static XmlConfigurator parse(InputStream stream) throws java.io.IOException {
        try {
            XmlNode root=parseXmlDocument(stream);
            return parse(root);
        }
        catch (Exception x) {
            throw new IOException(String.format(Util.getMessage("ParseError"), x.getLocalizedMessage()));
        }
    }

    private static InputStream getAsInputStreamFromClassLoader(String filename) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream is = cl == null ? null : cl.getResourceAsStream(filename);
        if (is == null) {
            // check system class loader
            is = XmlConfigurator.class.getClassLoader().getResourceAsStream(filename);
        }
        return is;
    }
    
    protected static XmlConfigurator parse(XmlNode root) throws Exception {
        return new XmlConfigurator(parseProtocols(root));
    }


    public static List<ProtocolConfiguration> parseProtocols(XmlNode root) throws Exception {
        String root_name=root.getName().trim().toLowerCase();
        if(!"config".equals(root_name))
            throw new IOException("the configuration does not start with a <config> element: " + root_name);

        List<ProtocolConfiguration> prot_data=new ArrayList<>();
        for(XmlNode node: root.getChildren()) {
            String  protocol=node.getName();
            Map<String,String> attrs=node.getAttributes();
            ProtocolConfiguration cfg=new ProtocolConfiguration(protocol, attrs);
            prot_data.add(cfg);

            // store protocol-specific configuration (if available); this will be passed to the protocol on
            // creation to to parse
            List<XmlNode> subnodes=node.getChildren();
            if(subnodes == null)
                continue;
            for(XmlNode subnode: subnodes) {
                cfg.addSubtree(subnode);
            }
        }
        return prot_data;
    }


    /** Reads XML and returns a simple tree of XmlNodes */
    public static XmlNode parseXmlDocument(InputStream in) throws IOException {
        Deque<XmlNode> stack=new ArrayDeque<>();
        XmlNode        current=null;
        while(true) {
            String s=readNode(in);
            if(s == null)
                break;

            s=sanitize(s);
            ElementType type=getType(s);
            if(type == ElementType.COMMENT)
                continue;

            // remove "<", "/> and ">"
            s=s.replace("<", "").replace( "/>", "").replace(">", "")
              .trim();

            InputStream input=new ByteArrayInputStream(s.getBytes());
            String name=Util.readToken(input);
            XmlNode n=new XmlNode(name);
            for(;;) {
                Tuple<String,String> tuple=readAttribute(input);
                if(tuple == null)
                    break;
                n.addAttribute(tuple.getVal1(), tuple.getVal2());
            }

            current=stack.peekFirst();
            switch(type) {
                case COMPLETE:
                    current.addChild(n);
                    break;
                case START:
                    if(current != null)
                        current.addChild(n);
                    stack.push(n);
                    break;
                case END:
                    stack.pop();
            }
        }
        return current;
    }


    protected static String readNode(InputStream in) throws IOException {
        String tmp=readTillMatchingCharacter(in, '<');
        if(tmp == null)
            return null;
        StringBuilder sb=new StringBuilder("<");
        tmp=readTillMatchingCharacter(in, '>');
        if(tmp == null)
            return null;
        sb.append(tmp);
        return sb.toString();
    }

    /** Fixes errors like "/  >" with "/>" */
    protected static String sanitize(String s) {
        return s.replaceAll("/\\s*>", "/>");
    }

    protected static Tuple<String,String> readAttribute(InputStream in) throws IOException {
        String attr_name=readTillMatchingCharacter(in, '=');
        if(attr_name == null)
            return null;
        attr_name=attr_name.replace("=", "").trim();
        String val=readQuotedString(in);
        if(val == null)
            return null;
        return new Tuple<>(attr_name, val);
    }

    /** Reads the characters between a start and end quote ("). Skips chars escaped with '\' */
    protected static String readQuotedString(InputStream in) throws IOException {
        String s=readTillMatchingCharacter(in, '"');
        if(s == null)
            return null;
        StringBuilder sb=new StringBuilder();
        boolean escaped=false;
        for(;;) {
            int ch=in.read();
            if(ch == -1)
                break;
            if(ch == '\\') {
                escaped=true;
                continue;
            }

            if(escaped)
                escaped=false;
            else if(ch == '"')
                break;
            sb.append((char)ch);
        }
        return sb.toString();
    }

    protected static ElementType getType(String s) {
        s=s.trim();
        if(s.startsWith("<!--"))
            return ElementType.COMMENT;
        if(s.startsWith("</"))
            return ElementType.END;
        if(s.endsWith("/>"))
            return ElementType.COMPLETE;
        if(s.endsWith(">"))
            return ElementType.START;
        return ElementType.UNDEFINED;
    }

    protected static boolean isClosed(String s) {
        s=s.trim();
        return s.startsWith("</") || s.endsWith("/>");
    }

    protected static boolean isComment(String s) {
        return s.trim().startsWith("<!--");
    }

    public static void main(String[] args) throws Exception {
        String input_file=null;
        XmlConfigurator conf;

        for(int i=0;i < args.length;i++) {
            if(args[i].equals("-file")) {
                input_file=args[++i];
                continue;
            }
            help();
            return;
        }

        if(input_file != null) {
            InputStream input=null;

            try {
                input=new FileInputStream(input_file);
            }
            catch(Throwable ignored) {
            }

            if(input == null)
                input=Thread.currentThread().getContextClassLoader().getResourceAsStream(input_file);

            conf=XmlConfigurator.getInstance(input);
            String tmp=conf.getProtocolStackString();
            System.out.println("\n" + tmp);
        }
        else
            throw new Exception("no input file given");
    }

    
    private static String dump(Collection<ProtocolConfiguration> configs) {
        StringBuilder sb=new StringBuilder();
        String indent="  ";
        sb.append("<config>\n");

        for(ProtocolConfiguration cfg: configs) {
            sb.append(indent).append("<").append(cfg.getProtocolName());
            Map<String,String> props=cfg.getProperties();
            if(!props.isEmpty()) {
                sb.append("\n").append(indent).append(indent);
                for(Map.Entry<String,String> entry : props.entrySet()) {
                    String key=entry.getKey();
                    String val=entry.getValue();
                    key=trim(key);
                    val=trim(val);
                    sb.append(key).append("=\"").append(val).append("\" ");
                }
            }
            sb.append(" />\n");
        }

        sb.append("</config>\n");
        return sb.toString();
    }

    private static String trim(String val) {
        StringBuilder retval=new StringBuilder();
        int index;

        val=val.trim();
        while(true) {
            index=val.indexOf('\n');
            if(index == -1) {
                retval.append(val);
                break;
            }
            retval.append(val, 0, index);
            val=val.substring(index + 1);
        }

        return retval.toString();
    }

    private static String inputAsString(InputStream input) throws IOException {
        int len=input.available();
        byte[] buf=new byte[len];
        input.read(buf, 0, len);
        return new String(buf);
    }

    public static String replace(String input, final String expr, String replacement) {
        StringBuilder sb=new StringBuilder();
        int new_index=0, index=0, len=expr.length(), input_len=input.length();

        while(true) {
            new_index=input.indexOf(expr, index);
            if(new_index == -1) {
                sb.append(input, index, input_len);
                break;
            }
            sb.append(input, index, new_index);
            sb.append(replacement);
            index=new_index + len;
        }

        return sb.toString();
    }

    static void help() {
        System.out.println("XmlConfigurator -file <input XML file>");
    }
}
