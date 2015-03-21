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
import java.util.Map;
import org.dbunit.dataset.IDataSet;
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
public class IdSequenceDaoTest {

    private DbUnitSupport support;
    private IDataSet queryDataSet;
    private IdSequenceDao dao;
    private HarvestTransaction transaction;

    public IdSequenceDaoTest() {
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
        queryDataSet = support.loadFlatXmlDataStream(this.getClass(), "IdSequenceDaoTestDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), queryDataSet);

        dao = new IdSequenceDao();
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
    public void testFind() throws Exception {
        transaction.begin();
        try {
            IdSequence id = dao.find(IdSequence.DIGOBJECT);
            assertNotNull(id);
            assertEquals(BigDecimal.ONE, id.getId());
            assertEquals(IdSequence.DIGOBJECT, id.getName());
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testFindIds() throws Exception {
        transaction.begin();
        try {
            Map<String, IdSequence> ids = dao.find(IdSequence.DIGOBJECT, IdSequence.METADATA);
            assertNotNull("ids", ids);
            IdSequence id = ids.get(IdSequence.DIGOBJECT);
            assertNotNull("id: " +  IdSequence.DIGOBJECT, id);
            assertEquals(BigDecimal.ONE, id.getId());
            assertEquals(IdSequence.DIGOBJECT, id.getName());

            id = ids.get(IdSequence.METADATA);
            assertNotNull("id: " +  IdSequence.METADATA, id);
            assertEquals(BigDecimal.valueOf(10), id.getId());
            assertEquals(IdSequence.METADATA, id.getName());
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testUpdate() throws Exception {
        transaction.begin();
        try {
            IdSequence result = dao.find(IdSequence.DIGOBJECT);
            assertNotNull(result);
            assertFalse(result.isNew());
            assertEquals(BigDecimal.ONE, result.getId());
            assertEquals(IdSequence.DIGOBJECT, result.getName());

            assertEquals(BigDecimal.valueOf(2), result.increment());
            assertEquals(BigDecimal.valueOf(3), result.increment());
            assertEquals(BigDecimal.valueOf(3), result.getId());

            dao.update(result);

            result = dao.find(IdSequence.DIGOBJECT);
            assertNotNull(result);
            assertFalse(result.isNew());
            assertEquals(BigDecimal.valueOf(3), result.getId());
            assertEquals(IdSequence.DIGOBJECT, result.getName());
        } finally {
            transaction.close();
        }
    }

    @Test
    public void testInsert() throws Exception {
        transaction.begin();
        try {
            IdSequence result = dao.find(IdSequence.RELATION);
            assertNull(result);

            IdSequence id = new IdSequence(IdSequence.RELATION);
            assertTrue(id.isNew());
            assertEquals(BigDecimal.ZERO, id.getId());
            assertEquals(IdSequence.RELATION, id.getName());
            assertEquals(BigDecimal.ONE, id.increment());

            dao.insert(id);

            result = dao.find(IdSequence.RELATION);
            assertNotNull(result);
            assertFalse(result.isNew());
            assertEquals(BigDecimal.ONE, result.getId());
            assertEquals(IdSequence.RELATION, result.getName());
        } finally {
            transaction.close();
        }
    }

}