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

import cz.registrdigitalizace.harvest.HarvestedRecord;
import java.math.BigDecimal;
import org.dbunit.Assertion;
import org.dbunit.dataset.ITable;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.SortedTable;
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
public class RelationDaoTest {
    private static final String RELATIONS_TABLE = "DIGVAZBY";
    private DbUnitSupport support;
    private HarvestTransaction transaction;
    private RelationDao dao;

    public RelationDaoTest() {
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
        dao = new RelationDao();
        dao.setDataSource(transaction);
    }

    @After
    public void tearDown() throws Exception {
        if (support != null) {
            support.close();
        }
    }

    @Test
    public void testDeleteRoot() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestDeleteDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestDeleteRootResult.xml", true);
        ITable expected = expectedDS.getTable(RELATIONS_TABLE);
        BigDecimal libraryId = BigDecimal.TEN;

        HarvestedRecord record = new HarvestedRecord();
        record.setId(BigDecimal.ONE);
        record.setUuid("u1");

        transaction.begin();
        try {
            try {
                dao.delete(record, libraryId);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
        ITable result = support.getConnection().createTable(RELATIONS_TABLE);
        Assertion.assertEquals(expected, result);
    }

    @Test
    public void testDelete() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestDeleteDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestDeleteResult.xml", true);
        ITable expected = expectedDS.getTable(RELATIONS_TABLE);
        BigDecimal libraryId = BigDecimal.TEN;

        HarvestedRecord record = new HarvestedRecord();
        record.setId(BigDecimal.valueOf(2));
        record.setUuid("u2");

        transaction.begin();
        try {
            try {
                dao.delete(record, libraryId);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
        ITable result = support.getConnection().createTable(RELATIONS_TABLE);
        result = new SortedTable(result); // Oracle does not sort this properly
//        support.dumpTable(result);
//        support.dumpTable(expected);
        Assertion.assertEquals(expected, result);
    }

    @Test
    public void testInsert() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestInsertDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "RelationDaoTestInsertResult.xml", true);
        ITable expected = expectedDS.getTable(RELATIONS_TABLE);
        BigDecimal libraryId = BigDecimal.TEN;
        IdSequenceDao idSequenceDao = new IdSequenceDao();
        idSequenceDao.setDataSource(transaction);

        HarvestedRecord rec1 = DigObjectDaoTest.createRecord(1, "u1", true, "periodical", "LN", "u2");
        HarvestedRecord rec2 = DigObjectDaoTest.createRecord(2, "u2", false, "volume", "LN - volume 1", "u10");
        HarvestedRecord rec3 = DigObjectDaoTest.createRecord(10, "u10", false, "issue", "LN - issue 1");

        transaction.begin();
        try {
            try {
//                IdSequence relationSequence = idSequenceDao.find(IdSequence.RELATION);
                IdSequence relationSequence = new IdSequence(BigDecimal.ZERO, IdSequence.RELATION);
                assertNotNull("relationSequence", relationSequence);
                dao.insert(relationSequence, rec1, libraryId);
                dao.insert(relationSequence, rec2, libraryId);
                dao.insert(relationSequence, rec3, libraryId);
                transaction.commit();
            } catch (DaoException ex) {
                transaction.rollback();
                throw new IllegalStateException(ex);
            }
        } finally {
            transaction.close();
        }
//        ITable result = support.getConnection().createQueryTable(RELATIONS_TABLE, "select * from RELATION order by PREDEK, POTOMEK");
        ITable result = support.getConnection().createTable(RELATIONS_TABLE);
//        support.dumpTable(expected);
//        support.dumpTable(result);
        Assertion.assertEquals(expected, result);
    }

}