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
package cz.registrdigitalizace.harvest.db;

import cz.registrdigitalizace.harvest.db.Metadata.MetadataItem;
import java.math.BigDecimal;
import java.util.Arrays;
import org.dbunit.Assertion;
import org.dbunit.dataset.ITable;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.dataset.IDataSet;
import org.hamcrest.CoreMatchers;
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
public class MetadataDaoTest {
    private DbUnitSupport support;
    private MetadataDao dao;
    private HarvestTransaction transaction;

    public MetadataDaoTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
        support = new DbUnitSupport();
        dao = new MetadataDao();
        transaction = new HarvestTransaction(support.getSource());
        dao.setDataSource(transaction);
    }

    @After
    public void tearDown() throws Exception {
        if (support != null) {
            support.close();
        }
    }
    
    @Test
    public void testDelete() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        BigDecimal libId = BigDecimal.valueOf(1);
        try {
            transaction.begin();
            dao.delete(libId);
            transaction.commit();
        } finally {
            transaction.close();
        }

        ITable result = support.getConnection().createQueryTable("METADATA",
                "select M.* from METADATA M, DIGOBJEKT D where RDIGOBJEKT_METADATA=D.ID and rdigknihovna_digobjekt=" + libId);
        assertEquals(0, result.getRowCount());
        ITable result2 = support.getConnection().createTable("METADATA");
        assertThat(result2.getRowCount(), CoreMatchers.not(0));
    }

    @Test
    public void testInsert() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestInsertResult.xml", true);

        Metadata m = createNewRecord(BigDecimal.valueOf(2), "DO2", "autori2",
                "vydavatele2", "issn2", "isbn2", "ccnb2", "sigla2", "signatura2", "rokvyd2");
        try {
            transaction.begin();
            dao.insert(new IdSequence(BigDecimal.valueOf(2), IdSequence.METADATA), m);
            transaction.commit();
        } finally {
            transaction.close();
        }
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"DIGKNIHOVNA", "DIGOBJEKT", "METADATA"});
//        support.printTableAsFlatXml("METADATA");
//        support.dumpTable(resultDS.getTable("METADATA"));
//        support.dumpTable(expectedDS.getTable("METADATA"));
        Assertion.assertEquals(expectedDS, resultDS);
    }

    @Test
    public void testInsertLargeValues() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        char[] chars = new char[5000];
        Arrays.fill(chars, 'č');
        String longString = String.valueOf(chars);
        Metadata m = createNewRecord(BigDecimal.valueOf(2), longString, longString,
                longString, longString, longString, longString, longString, longString, longString);
        try {
            transaction.begin();
            dao.insert(new IdSequence(BigDecimal.valueOf(2), IdSequence.METADATA), m);
            transaction.commit();
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testUpdate() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestUpdateResult.xml", true);

        Metadata m = createNewRecord(BigDecimal.valueOf(1), "Updated_DO1", null,
                null, null, null, null, null, null, null);
        try {
            transaction.begin();
            dao.delete(m);
            dao.insert(new IdSequence(BigDecimal.valueOf(2), IdSequence.METADATA), m);
            transaction.commit();
        } finally {
            transaction.close();
        }
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"DIGKNIHOVNA", "DIGOBJEKT", "METADATA"});
//        support.printTableAsFlatXml("METADATA");
//        support.dumpTable(resultDS.getTable("METADATA"));
//        support.dumpTable(expectedDS.getTable("METADATA"));
        Assertion.assertEquals(expectedDS, resultDS);
    }

    public static Metadata createNewRecord(BigDecimal id, String title,
            String authors, String publishers, String issn, String isbn,
            String ccnb, String sigla, String signature, String yearOfPublication) {

        Metadata m2 = new Metadata();
        m2.setDigObjId(id);
        addItem(m2, null, title, Metadata.NAZEV, null);
        addItem(m2, null, authors, Metadata.OSOBA, null);
        addItem(m2, null, publishers, Metadata.OSOBA, null);
        addItem(m2, null, issn, Metadata.ISSN, null);
        addItem(m2, null, isbn, Metadata.ISBN, null);
        addItem(m2, null, ccnb, Metadata.CCNB, null);
        addItem(m2, null, sigla, Metadata.SIGLA_BIB_UDAJU, null);
        addItem(m2, null, signature, Metadata.SIGNATURA, null);
        addItem(m2, null, yearOfPublication, Metadata.ROK_VYDANI, null);
        return m2;
    }

    public static void addItem(Metadata m2, BigDecimal id, String value, String reliefName, Boolean invalid) {
        if (value != null) {
            m2.getItems().add(new MetadataItem(id, value, reliefName, invalid));
        }
    }
}
