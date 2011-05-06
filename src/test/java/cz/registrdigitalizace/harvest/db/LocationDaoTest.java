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
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import org.dbunit.Assertion;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.operation.DatabaseOperation;
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
public class LocationDaoTest {
    private static final String LOCATIONS_TABLE = "LOKACE";
    private DbUnitSupport support;
    private LocationDao dao;
    private HarvestTransaction transaction;
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public LocationDaoTest() {
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
        dao = new LocationDao();
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
    public void testInsert() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "LocationDaoTestInsertDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        BigDecimal locationId = BigDecimal.TEN;
        BigDecimal digiObjectId = BigDecimal.TEN;
        BigDecimal libraryId = BigDecimal.TEN;
        Date inputDate = new Date(System.currentTimeMillis());

        transaction.begin();
        try {
            try {
                dao.insert(locationId, digiObjectId, libraryId, inputDate);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
//        support.printTableAsFlatXml("LOKACE");
        ITable lokaceTable = support.getConnection().createTable(LOCATIONS_TABLE);
//        support.dumpTable(lokaceTable);
        assertEquals(1, lokaceTable.getRowCount());
        assertEquals(locationId, lokaceTable.getValue(0, "ID"));
        assertEquals("examplelib", lokaceTable.getValue(0, "DIGKNIHOVNA"));
        assertEquals(digiObjectId, lokaceTable.getValue(0, "RDIGOBJEKTL"));
        assertEquals(dateFormat.format(inputDate), dateFormat.format(lokaceTable.getValue(0, "DATUMZAL")));
    }

    @Test
    public void testUpdate() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "LocationDaoTestUpdateDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "LocationDaoTestUpdateResult.xml");
        ITable expected = expectedDS.getTable(LOCATIONS_TABLE);
        BigDecimal locationId = BigDecimal.TEN;
        Date inputDate = inputDate = Date.valueOf("2011-03-10");

        transaction.begin();
        try {
            try {
                dao.update(locationId, inputDate);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }

        ITable result = support.getConnection().createQueryTable(LOCATIONS_TABLE, "select * from LOKACE order by id");
        result = DefaultColumnFilter.includedColumnsTable(result, expected.getTableMetaData().getColumns());
        Assertion.assertEquals(expected, result);
    }

    @Test
    public void testFind() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "LocationDaoTestUpdateDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        BigDecimal digiObjectId = BigDecimal.TEN;
        BigDecimal libraryId = BigDecimal.TEN;

        transaction.begin();
        try {
            try {
                BigDecimal result = dao.find(digiObjectId, libraryId);
                assertEquals(BigDecimal.TEN, result);
                result = dao.find(digiObjectId, BigDecimal.ONE);
                assertNull(result);
                result = dao.find(BigDecimal.ONE, libraryId);
                assertNull(result);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testDelete() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "LocationDaoTestUpdateDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        BigDecimal locationId = BigDecimal.TEN;

        transaction.begin();
        try {
            try {
                dao.delete(Arrays.asList(locationId));
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
        ITable result = support.getConnection().createTable(LOCATIONS_TABLE);
        assertEquals(1, result.getRowCount());
        result = support.getConnection().createQueryTable("deleteResult", "select * from LOKACE where id=" + locationId);
        assertEquals(0, result.getRowCount());
    }

}