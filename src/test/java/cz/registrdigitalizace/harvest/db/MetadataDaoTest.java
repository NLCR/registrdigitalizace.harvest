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

import java.math.BigDecimal;
import org.dbunit.Assertion;
import org.dbunit.dataset.ITable;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.dataset.IDataSet;
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
        try {
            transaction.begin();
            dao.delete();
            transaction.commit();
        } finally {
            transaction.close();
        }

        ITable result = support.getConnection().createTable("DIGMETADATA");
        assertEquals(0, result.getRowCount());
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
            dao.insert(m);
            transaction.commit();
        } finally {
            transaction.close();
        }
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"DIGOBJEKT", "DIGMETADATA"});
//        support.printTableAsFlatXml("DIGMETADATA");
//        support.dumpTable(resultDS.getTable("DIGMETADATA"));
//        support.dumpTable(expectedDS.getTable("DIGMETADATA"));
        Assertion.assertEquals(expectedDS, resultDS);
    }

    @Test
    public void testUpdate() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestUpdateResult.xml", true);

        Metadata m = createNewRecord(BigDecimal.valueOf(1), "Updated_DO1", "autori1",
                "vydavatele1", "issn1", "isbn1", "ccnb1", "sigla1", "signatura1", "rokvyd1");
        try {
            transaction.begin();
            dao.update(m);
            transaction.commit();
        } finally {
            transaction.close();
        }
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"DIGOBJEKT", "DIGMETADATA"});
//        support.printTableAsFlatXml("DIGMETADATA");
//        support.dumpTable(resultDS.getTable("DIGMETADATA"));
//        support.dumpTable(expectedDS.getTable("DIGMETADATA"));
        Assertion.assertEquals(expectedDS, resultDS);
    }

    @Test
    public void testUpdateDigObjectMetadata() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestUpdateDigObjMetDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "MetadataDaoTestUpdateDigObjMetResult.xml", true);

        try {
            transaction.begin();
            dao.updateDigObjectMetadata();
            transaction.commit();
        } finally {
            transaction.close();
        }
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{
            "DIGOBJEKT", "DIGVAZBY", "DIGMETADATA", "DIGMETADATA_CHANGES"});
        Assertion.assertEquals(expectedDS, resultDS);
    }

    public static Metadata createNewRecord(BigDecimal id, String title,
            String authors, String publishers, String issn, String isbn,
            String ccnb, String sigla, String signature, String yearOfPublication) {

        Metadata m = new Metadata();
        m.setAuthors(authors);
        m.setCcnb(ccnb);
        m.setId(id);
        m.setIsbn(isbn);
        m.setIssn(issn);
        m.setPublishers(publishers);
        m.setSigla(sigla);
        m.setSignature(signature);
        m.setTitle(title);
        m.setYearOfPublication(yearOfPublication);
        return m;
    }
}
