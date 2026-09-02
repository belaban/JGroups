package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validates that the generated native-image reachability metadata is consistent with the class
 * mappings defined in jg-magic-map.xml and jg-protocol-ids.xml.
 * @author Tristan Tarrant
 */
@Test(groups=Global.FUNCTIONAL)
public class NativeMetadataTest {

    private Set<String> magicMapClasses;
    private Set<String> protocolIdClasses;
    private Set<String> metadataClasses;
    private Set<String> metadataResources;

    @BeforeClass
    public void init() throws Exception {
        magicMapClasses = collectClassesFromXml(ClassConfigurator.MAGIC_NUMBER_FILE);
        protocolIdClasses = collectClassesFromXml(ClassConfigurator.PROTOCOL_ID_FILE);
        metadataClasses = new TreeSet<>();
        metadataResources = new TreeSet<>();
        parseMetadata();
    }

    public void testAllMagicMapClassesInMetadata() {
        Set<String> missing = new TreeSet<>(magicMapClasses);
        missing.removeAll(metadataClasses);
        assert missing.isEmpty() :
            "Classes in jg-magic-map.xml missing from native metadata: " + missing;
    }

    public void testAllProtocolIdClassesInMetadata() {
        Set<String> missing = new TreeSet<>(protocolIdClasses);
        missing.removeAll(metadataClasses);
        assert missing.isEmpty() :
            "Classes in jg-protocol-ids.xml missing from native metadata: " + missing;
    }

    public void testRequiredResourcesPresent() {
        List<String> required = List.of(
            ClassConfigurator.MAGIC_NUMBER_FILE,
            ClassConfigurator.PROTOCOL_ID_FILE,
            "JGROUPS_VERSION.properties",
            "jg-messages.properties"
        );
        for (String res : required) {
            assert metadataResources.contains(res) :
                "Required resource missing from native metadata: " + res;
        }
    }

    public void testMetadataHasNoBulkQueryConditions() throws Exception {
        String content = readMetadataFile();
        // Ensure no "condition" appears adjacent to "glob" or "proxy" patterns
        // which would violate GraalVM 25+ rules
        assert !content.contains("\"condition\"") :
            "Native metadata should not contain run-time conditions (\"condition\" entries)";
    }

    private Set<String> collectClassesFromXml(String resourceFile) throws Exception {
        Set<String> classes = new TreeSet<>();
        try (InputStream stream = Util.getResourceAsStream(resourceFile, getClass())) {
            String content = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            Pattern pattern = Pattern.compile("<class[^>]+name=\"([^\"]+)\"(?:[^>]+external=\"true\")?[^>]*/>");
            Matcher matcher = pattern.matcher(content);
            while (matcher.find()) {
                String className = matcher.group(1);
                boolean external = matcher.group(0).contains("external=\"true\"");
                if (!external) {
                    classes.add(className);
                }
            }
        }
        return classes;
    }

    private void parseMetadata() throws Exception {
        String content = readMetadataFile();
        // Extract type values from reflection entries
        Pattern typePattern = Pattern.compile("\"type\"\\s*:\\s*\"([^\"]+)\"");
        Matcher typeMatcher = typePattern.matcher(content);
        while (typeMatcher.find()) {
            metadataClasses.add(typeMatcher.group(1));
        }
        // Extract glob values from resource entries
        Pattern globPattern = Pattern.compile("\"glob\"\\s*:\\s*\"([^\"]+)\"");
        Matcher globMatcher = globPattern.matcher(content);
        while (globMatcher.find()) {
            metadataResources.add(globMatcher.group(1));
        }
    }

    private String readMetadataFile() throws Exception {
        try (InputStream stream = Util.getResourceAsStream(
                "META-INF/native-image/org.jgroups/jgroups/reachability-metadata.json", getClass())) {
            assert stream != null : "reachability-metadata.json not found on classpath";
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
