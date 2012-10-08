/*
 * Copyright (C) 2011 Jan Pokorsky
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package cz.registrdigitalizace.harvest.oai;

import java.net.URL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * XXX test fromDate
 *
 * @author Jan Pokorsky
 */
public class OaiSourceTest {

    private OaiSourceFactory factory;

    public OaiSourceTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        factory = OaiSourceFactory.getInstance();
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testPlainUrl() throws Exception {
        OaiSource inst = factory.createListRecords(
                "http://example.com:8080/oaiprovider/",
                null, null, null);
        doTestBuildUrl(inst,
                "http://example.com:8080/oaiprovider/?verb=ListRecords");
        doTestResumptionUrl(inst, "X30623170/1",
                "http://example.com:8080/oaiprovider/?verb=ListRecords&resumptionToken=X30623170/1");
    }

    @Test
    public void testUrlFormat() throws Exception {
        OaiSource inst = factory.createListRecords(
                "http://example.com:8080/oaiprovider/",
                null, "oai_dc", null);
        doTestBuildUrl(inst,
                "http://example.com:8080/oaiprovider/?verb=ListRecords&metadataPrefix=oai_dc");
        doTestResumptionUrl(inst, "X30623170/1",
                "http://example.com:8080/oaiprovider/?verb=ListRecords&resumptionToken=X30623170/1");
    }

    @Test
    public void testUrlOtherParams() throws Exception {
        OaiSource inst = factory.createListRecords(
                "http://example.com:8080/oaiprovider/",
                null, null, "set=type:periodical");
        doTestBuildUrl(inst,
                "http://example.com:8080/oaiprovider/?verb=ListRecords&set=type:periodical");
        doTestResumptionUrl(inst, "X30623170/1",
                "http://example.com:8080/oaiprovider/?verb=ListRecords&resumptionToken=X30623170/1");
    }

    @Test
    public void testBuildURL_Format_OtherParams() throws Exception {
        OaiSource inst = factory.createListRecords(
                "http://example.com:8080/oaiprovider/",
                null, "oai_dc", "set=type:periodical");
        doTestBuildUrl(inst,
                "http://example.com:8080/oaiprovider/?verb=ListRecords&metadataPrefix=oai_dc&set=type:periodical");
        doTestResumptionUrl(inst, "X30623170/1",
                "http://example.com:8080/oaiprovider/?verb=ListRecords&resumptionToken=X30623170/1");
    }

    private void doTestBuildUrl(OaiSource inst, String expectedURL) throws Exception {
        assertNotNull("inst", inst);
        URL url = inst.getUrl();
        assertNotNull("url", url);
        System.out.println("URL: " + url.toExternalForm());
        System.out.println("URI: " + url.toString());
        assertEquals(expectedURL, url.toString());
    }

    private void doTestResumptionUrl(OaiSource inst, String resumptionToken, String expectedURL) throws Exception {
        assertNotNull("inst", inst);
        URL url = inst.getResumptionUrl(resumptionToken);
        assertNotNull("url", url);
        System.out.println("URL: " + url.toExternalForm());
        System.out.println("URI: " + url.toString());
        assertEquals(expectedURL, url.toString());
    }

}