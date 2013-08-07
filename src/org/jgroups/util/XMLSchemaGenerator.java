package org.jgroups.util;

import org.jgroups.Version;
import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

/**
 * Iterates over all concrete Protocol classes and creates XML schema used for validation of
 * configuration files.
 * 
 * https://jira.jboss.org/jira/browse/JGRP-448
 * 
 * @author Vladimir Blagojevic
 * 
 */
public class XMLSchemaGenerator {

    public static void main(String[] args) {

        String outputDir = "./";

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-o".equals(arg)) {
                outputDir = args[++i];
            } else {
                System.out.println("XMLSchemaGenerator -o <path to newly created xsd schema file>");
                return;
            }
        }

        File f = new File(outputDir, "JGroups-" + Version.major + "." + Version.minor + ".xsd");
        try {
            FileWriter fw = new FileWriter(f, false);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            DOMImplementation impl = builder.getDOMImplementation();
            Document xmldoc = impl.createDocument("http://www.w3.org/2001/XMLSchema", "xs:schema",
                                                  null);
            xmldoc.getDocumentElement().setAttribute("targetNamespace", "urn:org:jgroups");
            xmldoc.getDocumentElement().setAttribute("elementFormDefault", "qualified");

            Element complexType = xmldoc.createElement("xs:complexType");
            complexType.setAttribute("name", "ConfigType");
            xmldoc.getDocumentElement().appendChild(complexType);

            Element allType = xmldoc.createElement("xs:choice");
            allType.setAttribute("maxOccurs", "unbounded");
            complexType.appendChild(allType);

            Set<Class<?>> classes = getClasses("org.jgroups.protocols", Protocol.class);
            for (Class<?> clazz : classes) {
                classToXML(xmldoc, allType, clazz, "");
            }
            classes = getClasses("org.jgroups.protocols.pbcast", Protocol.class);
            for (Class<?> clazz : classes) {
                classToXML(xmldoc, allType, clazz, "pbcast.");
            }

            classes = getClasses("org.jgroups.protocols.tom", Protocol.class);
            for (Class<?> clazz : classes) {
                classToXML(xmldoc, allType, clazz, "tom.");
            }

            classes = getClasses("org.jgroups.protocols.relay", Protocol.class);
            for (Class<?> clazz : classes) {
                classToXML(xmldoc, allType, clazz, "relay.");
            }

            classes = getClasses("org.jgroups.protocols.rules", Protocol.class);
            for (Class<?> clazz : classes) {
                classToXML(xmldoc, allType, clazz, "rules.");
            }


            Element xsElement = xmldoc.createElement("xs:element");
            xsElement.setAttribute("name", "config");
            xsElement.setAttribute("type", "ConfigType");
            xmldoc.getDocumentElement().appendChild(xsElement);

            DOMSource domSource = new DOMSource(xmldoc);
            StreamResult streamResult = new StreamResult(fw);
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer serializer = tf.newTransformer();
            serializer.setOutputProperty(OutputKeys.METHOD, "xml");
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
            serializer.transform(domSource, streamResult);

            fw.flush();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Set<Class<?>> getClasses(String packageName, Class<?> assignableFrom)
      throws IOException, ClassNotFoundException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Set<Class<?>> classes = new HashSet<Class<?>>();
        String path = packageName.replace('.', '/');
        URL resource = loader.getResource(path);
        if (resource != null) {
            String filePath = resource.getFile();
            if (filePath != null && new File(filePath).isDirectory()) {
                for (String file : new File(filePath).list()) {
                    if (file.endsWith(".class")) {
                        String name = packageName + '.' + file.substring(0, file.indexOf(".class"));
                        Class<?> clazz = Class.forName(name);
                        if (assignableFrom.isAssignableFrom(clazz))
                            classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }

    private static void classToXML(Document xmldoc, Element parent, Class<?> clazz,
                                   String preAppendToSimpleClassName) throws Exception {

        boolean isConcreteClass = (clazz.getModifiers() & Modifier.ABSTRACT) == 0;
        if (isConcreteClass && !clazz.isAnonymousClass()) {
            parent.appendChild(createXMLTree(xmldoc, clazz, preAppendToSimpleClassName));
        }
    }

    private static Element createXMLTree(Document xmldoc, Class<?> clazz,
                                         String preAppendToSimpleClassName) throws Exception {

        Element classElement = xmldoc.createElement("xs:element");
        String elementName = preAppendToSimpleClassName + clazz.getSimpleName();
        if(elementName == null || elementName.isEmpty()) {
            throw new IllegalArgumentException("Cannot create empty attribute name for element xs:element, class is " + clazz);
        }
        classElement.setAttribute("name",elementName);

        Element complexType = xmldoc.createElement("xs:complexType");
        classElement.appendChild(complexType);

        // iterate fields
        for (Class<?> clazzInLoop = clazz; clazzInLoop != null; clazzInLoop = clazzInLoop.getSuperclass()) {
            Field[] fields = clazzInLoop.getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(Property.class)) {
                    String property = field.getName();
                    Property r = field.getAnnotation(Property.class);
                    boolean annotationRedefinesName =!r.name().isEmpty()
                      && r.deprecatedMessage().isEmpty();
                    if (annotationRedefinesName) {
                        property = r.name();
                    }
                    if(property == null || property.isEmpty()) {
                        throw new IllegalArgumentException("Cannot create empty attribute name for element xs:attribute, field is " + field);
                    }
                    Element attributeElement = xmldoc.createElement("xs:attribute");
                    attributeElement.setAttribute("name", property);

                    // Agreement with Bela Ban on Jan-20-2009 (Go Obama!!!) to treat all types as
                    // xs:string since we do not know where users are going to use
                    // replacement tokens in configuration files. Therefore, the type becomes
                    // indeterminate.
                    // attributeElement.setAttribute("type", fieldToXMLSchemaAttributeType(field));
                    attributeElement.setAttribute("type", "xs:string");
                    complexType.appendChild(attributeElement);

                    Element annotationElement = xmldoc.createElement("xs:annotation");
                    attributeElement.appendChild(annotationElement);

                    Element documentationElement = xmldoc.createElement("xs:documentation");
                    documentationElement.setTextContent(r.description());
                    annotationElement.appendChild(documentationElement);
                }
            }
        }

        // iterate methods
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Property.class)) {

                Property annotation = method.getAnnotation(Property.class);
                String name = annotation.name();
                if (name.length() < 1) {
                    name = Util.methodNameToAttributeName(method.getName());
                }
                Element attributeElement = xmldoc.createElement("xs:attribute");
                attributeElement.setAttribute("name", name);
                attributeElement.setAttribute("type", "xs:string");
                complexType.appendChild(attributeElement);

                String desc = annotation.description();
                if (!desc.isEmpty()) {
                    Element annotationElement = xmldoc.createElement("xs:annotation");
                    attributeElement.appendChild(annotationElement);

                    Element documentationElement = xmldoc.createElement("xs:documentation");
                    documentationElement.setTextContent(annotation.description());
                    annotationElement.appendChild(documentationElement);
                }
            }
        }
        return classElement;
    }
}
