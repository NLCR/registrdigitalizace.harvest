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
package cz.registrdigitalizace.harvest.metadata;

import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import javax.xml.bind.JAXBException;
import javax.xml.transform.TransformerConfigurationException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
public class ModsMetadataParserTest {
    
    public ModsMetadataParserTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testParseDrobnustky() throws Exception {
        System.out.println("----- DrobnustkyMonographMods.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("DrobnustkyMonographMods.xml");
        DigobjectType result = doParse(modsxml);
        assertNotNull(result);
        String dump = "DrobnustkyMonographMods.xml";
        assertEquals(dump, "Drobnůstky", result.getTitle());
        assertEquals(dump, "ABA000", result.getSigla());
        assertEquals(dump, Arrays.asList("54 G 000887"), result.getSignature());
        assertEquals(dump, Arrays.asList("Doucha František"), result.getAuthor());
        assertEquals(dump, Arrays.asList("Mikoláš Lehmann 1862"), result.getPublisher());
        assertEquals(dump, Arrays.asList("1862"), result.getYear());
        assertNull(dump, result.getIsbn());
        assertNull(dump, result.getIssn());
        assertNull(dump, result.getCcnb());
    }

    @Test
    public void testParseKZ() throws Exception {
        System.out.println("----- KZMonographMods.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("KZMonographMods.xml");
        DigobjectType result = doParse(modsxml);
        assertNotNull(result);
        String dump = ModsMetadataParser.toString(result);
        assertEquals(dump,
                // title
                "Kniha zlatá, anebo, Nowý Zwěstowatel wsseho dobrého a vžitečného pro Národ Slowenský"
                    // subtitle
                    + " : " + "obsahugjcý: neydůležitěgssj a neyprospěssjěgssj k čtenj předměty, gak z popsánj zemj, .... práce básnjřské a giného wsseho obsahu zástoge",
                result.getTitle());
        assertEquals(dump, "ABA000", result.getSigla());
        assertEquals(dump, Arrays.asList("54 D 000254/D.1-2", "54 D 000254/D.1-2./Přív.1."), result.getSignature());
        assertEquals(dump, Arrays.asList("Kramerius Václav Rodomil"), result.getAuthor());
        assertEquals(dump, Arrays.asList(), result.getPublisher());
        assertEquals(dump, Arrays.asList(), result.getYear());
        assertNull(dump, result.getIsbn());
        assertNull(dump, result.getIssn());
        assertNull(dump, result.getCcnb());

        System.out.println("----- KZMonographUnit1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("KZMonographUnit1Mods.xml");
        result = doParse(modsxml);
        assertNotNull(result);
        dump = "KZMonographUnit1Mods.xml";
        assertEquals(dump, "Část: Zbirka 1", result.getTitle());
        assertEquals(dump, null, result.getSigla());
        assertEquals(dump, Arrays.asList(), result.getSignature());
        assertEquals(dump, Arrays.asList("Kramerius Václav"), result.getAuthor());
        assertEquals(dump, Arrays.asList("W.R. Kraméryus 1817"), result.getPublisher());
        assertEquals(dump, Arrays.asList("1817"), result.getYear());
        assertNull(dump, result.getIsbn());
        assertNull(dump, result.getIssn());
        assertNull(dump, result.getCcnb());
    }

    @Test
    public void testParseLN() throws Exception {
        System.out.println("----- LNPeriodicalMods.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNPeriodicalMods.xml");
        DigobjectType result = doParse(modsxml);
        assertNotNull(result);
        String dump = "LNPeriodicalMods.xml";
        assertEquals(dump, "Lidové noviny", result.getTitle());
        assertEquals(dump, "BOA001", result.getSigla());
        assertEquals(dump, Arrays.asList("Nov-20.730", "Skř.17-0849.021"), result.getSignature());
        assertEquals(dump, Arrays.asList(), result.getAuthor());
        assertEquals(dump,
                Arrays.asList("Vydavatelské družstvo Lidové strany v Brně 1893 - 1945",
                    "Pavel Váša a František Šelepa 1894 - 1919",
                    "Jaroslav Rejzek 1936 - 1945"),
                result.getPublisher());
        assertEquals(dump, Arrays.asList(), result.getYear());
        assertEquals(dump, null, result.getIsbn());
        assertEquals(dump, "1802-6265", result.getIssn());
        assertEquals(dump, null, result.getCcnb());
        
        System.out.println("----- LNVolume1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNVolume1Mods.xml");
        result = doParse(modsxml);
        assertNotNull(result);
        dump = "LNVolume1Mods.xml";
        assertEquals(dump, "Ročník: 1", result.getTitle());
        assertEquals(dump, null, result.getSigla());
        assertEquals(dump, Arrays.asList(), result.getSignature());
        assertEquals(dump, Arrays.asList(), result.getAuthor());
        assertEquals(dump, Arrays.asList(), result.getPublisher());
        assertEquals(dump, Arrays.asList("1893"), result.getYear());
        assertEquals(dump, null, result.getIsbn());
        assertEquals(dump, null, result.getIssn());
        assertEquals(dump, null, result.getCcnb());

        System.out.println("----- LNVolume1Issue1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNVolume1Issue1Mods.xml");
        result = doParse(modsxml);
        assertNotNull(result);
        dump = "LNVolume1Issue1Mods.xml";
        assertEquals(dump, "Číslo: 1", result.getTitle());
        assertEquals(dump, null, result.getSigla());
        assertEquals(dump, Arrays.asList(), result.getSignature());
        assertEquals(dump, Arrays.asList(), result.getAuthor());
        assertEquals(dump, Arrays.asList(), result.getPublisher());
        assertEquals(dump, Arrays.asList(), result.getYear());
        assertEquals(dump, null, result.getIsbn());
        assertEquals(dump, null, result.getIssn());
        assertEquals(dump, null, result.getCcnb());
    }

    private DigobjectType doParse(InputStream modsxml) throws IOException, JAXBException, TransformerConfigurationException {
        try {
            ModsMetadataParser instance = new ModsMetadataParser(ModsMetadataParser.MZK_STYLESHEET);
            DigobjectType result = instance.parse(new StreamSource(modsxml));
            return result;
        } finally {
            modsxml.close();
        }
    }
}
