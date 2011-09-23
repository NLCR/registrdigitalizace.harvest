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

import java.util.Arrays;
import cz.registrdigitalizace.harvest.HarvestedRecord;
import java.math.BigDecimal;
import org.dbunit.Assertion;
import org.dbunit.database.QueryDataSet;
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
//@Ignore
public class DigObjectDaoTest {
    private static final String DIGIOBJECTS_TABLE = "digobjekt";
    private DbUnitSupport support;
    private DigObjectDao dao;
    private HarvestTransaction transaction;

    public DigObjectDaoTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        support = new DbUnitSupport();
        dao = new DigObjectDao();
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
        QueryDataSet queryDataSet = new QueryDataSet(support.getConnection());
        queryDataSet.addTable(DIGIOBJECTS_TABLE);
        IDataSet initDataSet = support.loadFlatXmlDataStream(
                getClass(), "emptyDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataSet);
        transaction.begin();
        try {
            String uuid = "uuid:1";
            String type = "monograph";
            String xml = "<record></record>";
            try {
                dao.insert(BigDecimal.valueOf(1), uuid, type, xml);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
        int rowCount = queryDataSet.getTable(DIGIOBJECTS_TABLE).getRowCount();
        assertEquals(1, rowCount);
    }

    @Test
    public void testInsertExisting() throws Exception {
        QueryDataSet queryDataSet = new QueryDataSet(support.getConnection());
        queryDataSet.addTable(DIGIOBJECTS_TABLE);
        IDataSet initDataSet = support.loadFlatXmlDataStream(
                getClass(), "DigObjectTestFindDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataSet);
        transaction.begin();
        try {
            String uuid = "uuid:1";
            String type = "monograph";
            String xml = "<record></record>";
            try {
                dao.insert(BigDecimal.valueOf(1), uuid, type, xml);
                fail("duplicate key value violates unique constraint \"digobjekt_pkey\"");
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
            }
        } finally {
            transaction.close();
        }
        ITable resultTable = queryDataSet.getTable(DIGIOBJECTS_TABLE);
        int rowCount = resultTable.getRowCount();
        assertEquals(1, rowCount);
        ITable expectedTable = initDataSet.getTable(DIGIOBJECTS_TABLE);
        ITable filteredResultTable = DefaultColumnFilter.includedColumnsTable(
                resultTable, expectedTable.getTableMetaData().getColumns());
        Assertion.assertEquals(expectedTable, filteredResultTable);
    }

    @Test
    public void testFind() throws Exception {
        IDataSet initDataSet = support.loadFlatXmlDataStream(
                getClass(), "DigObjectTestFindDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataSet);
        transaction.begin();
        try {
            String uuid = "uuid:1";
            try {
                BigDecimal result = dao.find(uuid);
                assertEquals(BigDecimal.ONE, result);
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
    public void testFindMissingUuid() throws Exception {
        QueryDataSet queryDataSet = new QueryDataSet(support.getConnection());
        queryDataSet.addTable(DIGIOBJECTS_TABLE);
        IDataSet initDataSet = support.loadFlatXmlDataStream(
                getClass(), "emptyDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataSet);
        transaction.begin();
        try {
            String uuid = "uuid:1";
            try {
                BigDecimal result = dao.find(uuid);
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

    public static HarvestedRecord createNewRecord(String uuid, boolean root, String type, String name, String... relations) {
        return createRecord(null, uuid, root, type, name, relations);
    }

    public static HarvestedRecord createRecord(long id, String uuid, boolean root, String type, String name, String... relations) {
        return createRecord(BigDecimal.valueOf(id), uuid, root, type, name, relations);
    }

    public static HarvestedRecord createRecord(BigDecimal id, String uuid, boolean root, String type, String name, String... relations) {
        HarvestedRecord r = new HarvestedRecord();
        r.setId(id);
        r.setRoot(root);
        r.setType(type);
        r.setUuid(uuid);
        r.setDescriptor(String.format("<record root='%s'><uuid>%s<uuid><name>%s</name></record>", root, uuid, name));
        r.setChildren(Arrays.asList(relations));
        r.setMetadata(MetadataDaoTest.createNewRecord(null, name, null, null, null, null, null, null, null, null));
        return r;
    }

}