package org.jgroups.tests;

import org.jgroups.Version;
import org.jgroups.Global;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * @author Bela Ban April 4 2003
 * @version $Revision: 1.2 $
 */
@Test(groups=Global.FUNCTIONAL)
public class VersionTest {

    public void testIfManifestContainsImplementationVersion() throws Exception {
        //given
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/jgroups-version.properties"));

        //when
        String version = properties.getProperty("version");

        //then
        assertNotNull(version);
    }

    public void testIfVersionWasExtracted() throws Exception {
        //when
        String version = Version.printDescription();

        //then
        assertTrue(version.matches("JGroups [1-9].\\d.\\d.Final"), "Extracted version: " + version);
    }

    public void testVersionPrint() {
        assertNotNull(Version.printVersion());
    }

    public void testNullVersion() {
        assert !(Version.isSame((short)0));
    }

    public void testDifferentLengthVersion1() {
        short version2=Version.encode(2,0,7);
        assert !(Version.isSame(version2));
    }

    public void testDifferentVersion() {
        short version1=Version.encode(2,0,7), version2=Version.encode(2,0,6);
        assert !(version1 == version2);
    }

    public void testSameVersion() {
        assert match(0,0,1, 0,0,1);
        assert match(1,0,0, 1,0,0);
        assert match(10,2,60, 10,2,60);
        assert !(match(1, 2, 3, 1, 2, 0));
        assert !(match(0, 0, 0, 0, 0, 1));
        assert !(match(2, 5, 0, 2, 5, 1));
    }

    public void testBinaryCompatibility() {
        assert isBinaryCompatible(0,0,0, 0,0,0);
        assert isBinaryCompatible(1,2,0, 1,2,1);
        assert isBinaryCompatible(1,2,0, 1,2,60);
        assert !(isBinaryCompatible(2, 5, 0, 2, 4, 1));
        assert !(isBinaryCompatible(2, 5, 0, 2, 6, 0));
    }

    private boolean match(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        return version1 == version2;
    }

    private boolean isBinaryCompatible(int major_1, int minor_1, int micro_1, int major_2, int minor_2, int micro_2) {
        short version1=Version.encode(major_1, minor_1, micro_1);
        short version2=Version.encode(major_2, minor_2, micro_2);
        boolean retval=Version.isBinaryCompatible(version1, version2);
        System.out.println(Version.print(version1) + " binary compatible to " + Version.print(version2) + (retval? " OK" : " FAIL"));
        return Version.isBinaryCompatible(version1, version2);
    }

}
