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

import cz.registrdigitalizace.harvest.db.ThumbnailDao.Thumbnail;
import org.dbunit.operation.DatabaseOperation;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.NoSuchElementException;
import org.dbunit.Assertion;
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
public class ThumbnailDaoTest {

    private DbUnitSupport support;
    private HarvestTransaction transaction;

    public ThumbnailDaoTest() {
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
        transaction = new HarvestTransaction(support.getSource());
    }

    @After
    public void tearDown() throws Exception {
        if (support != null) {
            support.close();
        }
    }

    @Test
    public void testInsert() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "ThumbnailDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "ThumbnailDaoTestInsertResult.xml", true);

        BigDecimal digiObjId = BigDecimal.ONE;
        String mimeType = "image/jpeg";
        byte[] buf = "karel".getBytes("UTF-8");
        InputStream contents = new ByteArrayInputStream(buf);
        Long length = null;
        ThumbnailDao instance = new ThumbnailDao();
        instance.setDataSource(transaction);
        transaction.begin();
//        instance.insert(digiObjId, mimeType, buf);
        instance.insert(digiObjId, mimeType, contents, buf.length);
        transaction.commit();
        transaction.close();
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"THUMBNAILS"});
//        support.printTableAsFlatXml("THUMBNAILS");
//        support.dumpTable(resultDS.getTable("THUMBNAILS"));
//        support.dumpTable(expectedDS.getTable("THUMBNAILS"));
        Assertion.assertEquals(expectedDS, resultDS);
    }

    @Test
    public void testDeleteUnrelated() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "ThumbnailDaoTestDeleteDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);

        ThumbnailDao instance = new ThumbnailDao();
        instance.setDataSource(transaction);
        transaction.begin();

        instance.deleteUnrelated();

        transaction.commit();
        transaction.close();

        assertEquals(0, support.getConnection().getRowCount("THUMBNAILS"));
    }

    @Test
    public void testFindMissing() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "ThumbnailDaoTestFindMissingDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);

        ThumbnailDao instance = new ThumbnailDao();
        instance.setDataSource(transaction);
        transaction.begin();

        IterableResult<Thumbnail> result = instance.findMissing();
        assertNotNull(result);
        assertTrue(result.hasNextResult());
        Thumbnail nextResult = result.nextResult();
        assertNotNull(nextResult);
        assertEquals(BigDecimal.ONE, nextResult.getDigiObjId());
        assertEquals(BigDecimal.TEN, nextResult.getLibraryId());
        assertEquals("uuid:1", nextResult.getUuid());

        assertFalse(result.hasNextResult());
        try {
            nextResult = result.nextResult();
            fail("unexpected next: " + nextResult);
        } catch (NoSuchElementException ex) {
            // expected
        }

        result.close();

        transaction.commit();
        transaction.close();

        assertEquals(1, support.getConnection().getRowCount("THUMBNAILS"));
    }

}