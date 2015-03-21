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

import cz.registrdigitalizace.harvest.db.Metadata;
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
import static cz.registrdigitalizace.harvest.TestUtils.*;
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
        Metadata m = doParse(modsxml);
        assertNotNull(m);
        assertEquals(Arrays.asList("Drobnůstky"), m.find(Metadata.NAZEV));
        assertSetEquals(asSet(), m.find(Metadata.CISLO));
        assertSetEquals(Arrays.asList("ABA000"), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(Arrays.asList("54 G 000887"), m.find(Metadata.SIGNATURA));
        assertSetEquals(Arrays.asList("Doucha", "František"), m.find(Metadata.OSOBA));
//        assertSetEquals(Arrays.asList("Mikoláš Lehmann 1862"), m.find(Metadata.OSOBA)); // Publisher
        assertSetEquals(Arrays.asList("1862"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(asSet(), m.find(Metadata.ISSN));
        assertSetEquals(asSet(), m.find(Metadata.ISBN));
        assertSetEquals(asSet(), m.find(Metadata.CCNB));
    }

    @Test
    public void testParseKZ() throws Exception {
        System.out.println("----- KZMonographMods.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("KZMonographMods.xml");
        Metadata m = doParse(modsxml);
        assertNotNull(m);
        String dump = m.toString();
        assertSetEquals(Arrays.asList(
                "Kniha zlatá, anebo, Nowý Zwěstowatel wsseho dobrého a vžitečného pro Národ Slowenský"
//                    // subtitle
//                    + " : " + "obsahugjcý: neydůležitěgssj a neyprospěssjěgssj k čtenj předměty, gak z popsánj zemj, .... práce básnjřské a giného wsseho obsahu zástoge",
            ), m.find(Metadata.NAZEV));
        assertSetEquals(dump, asSet(), m.find(Metadata.CISLO));
        assertSetEquals(dump, Arrays.asList("ABA000"), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(dump, Arrays.asList("54 D 000254/D.1-2", "54 D 000254/D.1-2./Přív.1."), m.find(Metadata.SIGNATURA));
        assertSetEquals(dump, Arrays.asList("Kramerius", "Václav", "Rodomil"), m.find(Metadata.OSOBA));
        assertSetEquals(dump, Arrays.asList(), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISSN));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISBN));
        assertSetEquals(dump, asSet(), m.find(Metadata.CCNB));

        System.out.println("----- KZMonographUnit1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("KZMonographUnit1Mods.xml");
        m = doParse(modsxml);
        assertNotNull(m);
        dump = "KZMonographUnit1Mods.xml";
        assertSetEquals(Arrays.asList(
                "Kniha zlatá, anebo, Nowý Zwěstowatel wsseho dobrého a vžitečného pro Národ Slowenský.",
                "Kniha zlatá, anebo, Nový Zvěstovatel všeho dobrého a užitečného pro Národ Slovenský"
            ), m.find(Metadata.NAZEV));
//        assertEquals(dump, "Část: Zbirka 1", m.getTitle());
        assertSetEquals(dump, Arrays.asList(), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(dump, Arrays.asList(), m.find(Metadata.SIGNATURA));
        assertSetEquals(dump, Arrays.asList("Kramerius", "Václav"), m.find(Metadata.OSOBA));
        assertSetEquals(dump, Arrays.asList("1817"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISSN));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISBN));
        assertSetEquals(dump, asSet(), m.find(Metadata.CCNB));
    }

    @Test
    public void testParseLN() throws Exception {
        System.out.println("----- LNPeriodicalMods.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNPeriodicalMods.xml");
        Metadata m = doParse(modsxml);
        assertNotNull(m);
        String dump = "LNPeriodicalMods.xml";
        assertSetEquals(Arrays.asList(
                "Lidové noviny"
            ), m.find(Metadata.NAZEV));
        assertSetEquals(dump, asSet(), m.find(Metadata.CISLO));
        assertSetEquals(dump, asSet("BOA001"), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(dump, asSet("Nov-20.730", "Skř.17-0849.021"), m.find(Metadata.SIGNATURA));
        assertSetEquals(dump, asSet(), m.find(Metadata.OSOBA));
//        assertEquals(dump,
//                Arrays.asList("Vydavatelské družstvo Lidové strany v Brně 1893 - 1945",
//                    "Pavel Váša a František Šelepa 1894 - 1919",
//                    "Jaroslav Rejzek 1936 - 1945"),
//                m.getPublisher());
        assertSetEquals(dump, asSet("1893 - 1945", "1936 - 1945", "1894 - 1919"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(dump, asSet("1802-6265"), m.find(Metadata.ISSN));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISBN));
        assertSetEquals(dump, asSet(), m.find(Metadata.CCNB));

        System.out.println("----- LNVolume1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNVolume1Mods.xml");
        m = doParse(modsxml);
        assertNotNull(m);
        dump = "LNVolume1Mods.xml";
        assertSetEquals(Arrays.asList(
//                "Ročník: 1"
            ), m.find(Metadata.NAZEV));
        assertSetEquals(dump, asSet("1"), m.find(Metadata.CISLO));
        assertSetEquals(dump, asSet(), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(dump, asSet(), m.find(Metadata.SIGNATURA));
        assertSetEquals(dump, asSet(), m.find(Metadata.OSOBA));
        assertSetEquals(dump, asSet("1893"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISSN));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISBN));
        assertSetEquals(dump, asSet(), m.find(Metadata.CCNB));

        System.out.println("----- LNVolume1Issue1Mods.xml");
        modsxml = ModsMetadataParserTest.class.getResourceAsStream("LNVolume1Issue1Mods.xml");
        m = doParse(modsxml);
        assertNotNull(m);
        dump = "LNVolume1Issue1Mods.xml";
        assertSetEquals(Arrays.asList(
//                "Číslo: 1"
            ), m.find(Metadata.NAZEV));
        assertSetEquals(dump, asSet("1"), m.find(Metadata.CISLO));
        assertSetEquals(dump, asSet(), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(dump, asSet(), m.find(Metadata.SIGNATURA));
        assertSetEquals(dump, asSet(), m.find(Metadata.OSOBA));
        assertSetEquals(dump, asSet("16.12.1893"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISSN));
        assertSetEquals(dump, asSet(), m.find(Metadata.ISBN));
        assertSetEquals(dump, asSet(), m.find(Metadata.CCNB));
    }

    @Test
    public void testMods3Metadata() throws Exception {
        System.out.println("----- mods3-metadata.xml");
        InputStream modsxml = ModsMetadataParserTest.class.getResourceAsStream("mods3-metadata.xml");
        Metadata m = doParse(modsxml);
        assertNotNull(m);
        assertSetEquals(Arrays.asList("urn:nbn:cz:nk-0010vt"), m.find(Metadata.URNNBN));
        assertSetEquals(asSet("1802-6265-invalid", "1802-6265"), m.find(Metadata.ISSN));
        assertSetEquals(Arrays.asList("1802-6265"), m.find(Metadata.ISSN, false));
        assertSetEquals(Arrays.asList("1802-6265-invalid"), m.find(Metadata.ISSN, true));
        assertSetEquals(Arrays.asList("Rohn, Johann Carl von Reichenberg"), m.find(Metadata.OSOBA, false));
        assertSetEquals(Arrays.asList("BOA001"), m.find(Metadata.SIGLA_FYZ_JEDNOTKY));
        assertSetEquals(Arrays.asList("Nov-20.730", "Skř.17-0849.021"), m.find(Metadata.SIGNATURA));
        assertSetEquals(Arrays.asList("Lidové noviny"), m.find(Metadata.NAZEV));
        assertSetEquals(Arrays.asList("1893 - 1945", "1894 - 1919", "1936 - 1945"), m.find(Metadata.ROK_VYDANI));
        assertSetEquals(Arrays.asList("BOA001"), m.find(Metadata.SIGLA_BIB_UDAJU));
        assertSetEquals(Arrays.asList("CZ-PrNK"), m.find(Metadata.KATALOG));
        assertSetEquals(Arrays.asList("bkn20031248927"), m.find(Metadata.POLE001));
        assertSetEquals(asSet("2"), m.find(Metadata.CISLO));
    }

    private Metadata doParse(InputStream modsxml) throws IOException, JAXBException, TransformerConfigurationException {
        try {
            ModsMetadataParser instance = new ModsMetadataParser(ModsMetadataParser.STYLESHEET);
            Metadata result = instance.parse(new StreamSource(modsxml));
            return result;
        } finally {
            modsxml.close();
        }
    }

}
