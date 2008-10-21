package org.jgroups.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Iterates over all concrete Protocol classes and creates an XML file per
 * Protocol class.
 * 
 * <p>
 * Each XML file in turn contains all protocol properties along with property
 * description. The output XML snipet is conforming docbook format so that
 * property descriptions can be easily added to master docbook JGroups
 * documentation.
 * 
 * @author Vladimir Blagojevic
 * @version $Id: PropertiesToXML.java,v 1.3 2008/10/21 13:19:41 vlada Exp $
 * 
 */
public class PropertiesToXML {

    public static void main(String[] args) {

        try {
            Set<Class<?>> classes=getClasses("org.jgroups.protocols", Protocol.class);
            classes.addAll(getClasses("org.jgroups.protocols.pbcast", Protocol.class));
            for(Class<?> clazz:classes) {
                classToXML(clazz);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    private static Set<Class<?>> getClasses(String packageName, Class<?> assignableFrom) throws IOException,
                                                                                        ClassNotFoundException {
        ClassLoader loader=Thread.currentThread().getContextClassLoader();
        Set<Class<?>> classes=new HashSet<Class<?>>();
        String path=packageName.replace('.', '/');
        URL resource=loader.getResource(path);
        if(resource != null) {
            String filePath=resource.getFile();
            if(filePath != null && new File(filePath).isDirectory()) {
                for(String file:new File(filePath).list()) {
                    if(file.endsWith(".class")) {
                        String name=packageName + '.' + file.substring(0, file.indexOf(".class"));
                        Class<?> clazz=Class.forName(name);
                        if(assignableFrom.isAssignableFrom(clazz))
                            classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }

    private static void classToXML(Class<?> clazz) throws ParserConfigurationException,
                                                  TransformerException {
        boolean isConcreteClass=(clazz.getModifiers() & Modifier.ABSTRACT) == 0;
        boolean isExperimental=clazz.isAnnotationPresent(Experimental.class);
        boolean isUnsupported=clazz.isAnnotationPresent(Unsupported.class);
        if(isConcreteClass && !isExperimental && !isUnsupported) {
            Class<?> protocol=clazz;
            Document xmldoc=null;
            DocumentBuilderFactory factory=DocumentBuilderFactory.newInstance();
            DocumentBuilder builder=factory.newDocumentBuilder();
            DOMImplementation impl=builder.getDOMImplementation();
            xmldoc=impl.createDocument(null, "table", null);
            Element tbody=createXMLTree(xmldoc);
            int propertyCount = 0;

            for(;clazz != null;clazz=clazz.getSuperclass()) {
                Field[] fields=clazz.getDeclaredFields();
                for(Field field:fields) {
                    if(field.isAnnotationPresent(Property.class)) {
                        Element row=xmldoc.createElement("row");                        
                        String property=field.getName();
                        Element entry=xmldoc.createElement("entry");
                        entry.setTextContent(property);
                        row.appendChild(entry);

                        Property annotation=field.getAnnotation(Property.class);
                        String desc=annotation.description();
                        entry=xmldoc.createElement("entry");
                        entry.setTextContent(desc);
                        row.appendChild(entry);
                        propertyCount++;
                        
                        tbody.appendChild(row);

                        //System.out.println(protocol + "#" + property + "=" + desc);
                    }
                }
            }
            //do we have more than one property (superclass Protocol has only one property (stats))
            if(propertyCount > 1) {
                DOMSource domSource=new DOMSource(xmldoc);
                StreamResult streamResult=new StreamResult(new File(protocol.getSimpleName() + ".xml"));
                TransformerFactory tf=TransformerFactory.newInstance();
                Transformer serializer=tf.newTransformer();
                serializer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
                serializer.setOutputProperty(OutputKeys.INDENT, "yes");
                serializer.transform(domSource, streamResult);
            }
        }
    }

    private static Element createXMLTree(Document xmldoc) throws ParserConfigurationException {

        Element root=xmldoc.getDocumentElement();
        Element title=xmldoc.createElement("title");
        title.setTextContent("Properties");
        root.appendChild(title);

        Element tgroup=xmldoc.createElement("tgroup");
        tgroup.setAttribute("cols", "2");
        root.appendChild(tgroup);

        Element colspec=xmldoc.createElement("colspec");
        colspec.setAttribute("align", "left");
        tgroup.appendChild(colspec);

        Element thead=xmldoc.createElement("thead");
        tgroup.appendChild(thead);

        Element row=xmldoc.createElement("row");
        thead.appendChild(row);

        Element entry=xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Name");
        row.appendChild(entry);

        entry=xmldoc.createElement("entry");
        entry.setAttribute("align", "center");
        entry.setTextContent("Description");
        row.appendChild(entry);

        Element tbody=xmldoc.createElement("tbody");
        tgroup.appendChild(tbody);        

        return tbody;
    }
}
