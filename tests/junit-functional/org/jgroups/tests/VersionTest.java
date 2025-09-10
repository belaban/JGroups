package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Version;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Bela Ban April 4 2003
 * @version $Revision: 1.2 $
 */
@Test(groups=Global.FUNCTIONAL)
public class VersionTest {

    @AfterMethod
    public void cleanupProperties() {
        System.clearProperty(Global.VERSION_CHECK);
        System.clearProperty(Global.VERSION_CHECK_MICRO);
        Version.initConfig();
    }

    public void testBinaryCompatibilityWithoutVersionCheck() {
        System.setProperty(Global.VERSION_CHECK, "false");
        Version.initConfig();
        // Everything should be binary compatible now as VERSION_CHECK is disabled
        assert isBinaryCompatible(0, 0, 0, 0, 0, 0);
        assert isBinaryCompatible(1, 2, 0, 1, 2, 1);
        assert isBinaryCompatible(1, 2, 0, 1, 2, 60);
        assert isBinaryCompatible(2, 5, 0, 2, 4, 1);
        assert isBinaryCompatible(2, 5, 0, 2, 6, 0);
    }

    public void testBinaryCompatibilityWithVersionCheckMicro() {
        System.setProperty(Global.VERSION_CHECK, "true");
        System.setProperty(Global.VERSION_CHECK_MICRO, "true");
        Version.initConfig();
        // Only exact same version should be considered compatible as VERSION_CHECK_MICRO is enabled
        assert isBinaryCompatible(0, 0, 0, 0, 0, 0);
        assert isBinaryCompatible(5, 3, 1, 5, 3, 1);
        // Everything that differs is considered incompatible as VERSION_CHECK_MICRO is enabled
        assert !isBinaryCompatible(1, 2, 0, 1, 2, 1);
        assert !isBinaryCompatible(1, 2, 0, 1, 2, 60);
        assert !isBinaryCompatible(2, 5, 0, 2, 4, 1);
        assert !isBinaryCompatible(2, 5, 0, 2, 6, 0);
    }

    public void testBinaryCompatibilityWithVersionCheckMicroButVersionCheckFalse() {
        System.setProperty(Global.VERSION_CHECK, "false");
        System.setProperty(Global.VERSION_CHECK_MICRO, "true");
        Version.initConfig();
        // Everything should be binary compatible now as VERSION_CHECK is disabled, regardless of VERSION_CHECK_MICRO
        assert isBinaryCompatible(0, 0, 0, 0, 0, 0);
        assert isBinaryCompatible(5, 3, 1, 5, 3, 1);
        assert isBinaryCompatible(1, 2, 0, 1, 2, 1);
        assert isBinaryCompatible(1, 2, 0, 1, 2, 60);
        assert isBinaryCompatible(2, 5, 0, 2, 4, 1);
        assert isBinaryCompatible(2, 5, 0, 2, 6, 0);
    }

    public void testIfManifestContainsImplementationVersion() throws Exception {
        Properties properties=new Properties();
        properties.load(Util.getResourceAsStream(Version.VERSION_FILE, getClass()));
        String version=properties.getProperty(Version.VERSION_PROPERTY);
        assertNotNull(version);
    }

    public void testIfVersionWasExtracted() throws Exception {
        String version=Version.printDescription();
        assertTrue(version.matches("JGroups [1-9].\\d.\\d.*"), "Extracted version: " + version);
    }
    public void testVersionPrint() {
        assertNotNull(Version.printVersion());
    }

    public static void testNullVersion() {
        assert !(Version.isSame((short)0));
    }


    public static void testDifferentLengthVersion1() {
        short version2=Version.encode(2,0,7);
        assert !(Version.isSame(version2));
    }

    public static void testDifferentVersion() {
        short version1=Version.encode(2,0,7), version2=Version.encode(2,0,6);
        assert !(version1 == version2);
    }

    public static void testSameVersion() {
        assert match(0,0,1, 0,0,1);
        assert match(1,0,0, 1,0,0);
        assert match(10,2,60, 10,2,60);
        assert !(match(1, 2, 3, 1, 2, 0));
        assert !(match(0, 0, 0, 0, 0, 1));
        assert !(match(2, 5, 0, 2, 5, 1));
    }

    public static void testBinaryCompatibility() {
        assert isBinaryCompatible(0,0,0, 0,0,0);
        assert isBinaryCompatible(1,2,0, 1,2,1);
        assert isBinaryCompatible(1,2,0, 1,2,60);
        assert !(isBinaryCompatible(2, 5, 0, 2, 4, 1));
        assert !(isBinaryCompatible(2, 5, 0, 2, 6, 0));
    }

    public void testIncrementVersion() {
        String[] versions={"5.5.0.Final-SNAPSHOT", "5.5.0.Final", "5.5.0", "5.5.0-SNAPSHOT"};

        for(String curr: versions) {
            short[] decoded=Version.decode(Version.parse(curr));
            assert Arrays.equals(decoded, new short[]{5, 5, 0});

            String next=Version.incrementVersion(curr);
            decoded=Version.decode(Version.parse(next));
            assert Arrays.equals(decoded, new short[]{5, 5, 1});
        }
    }


    private static boolean match(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        return version1 == version2;
    }

    private static boolean isBinaryCompatible(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        boolean retval=Version.isBinaryCompatible(version1, version2);
        System.out.println(Version.print(version1) + " binary compatible to " + Version.print(version2) + (retval? " OK" : " FAIL"));
        return Version.isBinaryCompatible(version1, version2);
    }



}
