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
import java.util.List;
import org.dbunit.Assertion;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Jan Pokorsky
 */
//@Ignore
public class LibraryDaoTest {
    private DbUnitSupport support;
    private LibraryDao dao;
    private HarvestTransaction transaction;

    public LibraryDaoTest() {
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
        transaction = new HarvestTransaction(support.getSource());
        transaction.begin();
        dao = new LibraryDao();
        dao.setDataSource(transaction);
    }

    @After
    public void tearDown() throws Exception {
        if (transaction != null) {
            transaction.close();
        }
        if (support != null) {
            support.close();
        }
    }

//    @Test
    public void loadDataSet() throws Exception {
        support.printTableAsFlatXml("DIGKNIHOVNA");
    }

    @Test
    public void testFind() throws Exception {
        IDataSet initDataStream = support.loadFlatXmlDataStream(getClass(), "LibraryDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataStream);

        List<Library> result = dao.find();
        assertNotNull(result);
        assertEquals(1, result.size());
        Library lib = result.get(0);
        assertEquals("http://example.com/oaiprovider", lib.getBaseUrl());
        assertEquals("oaipmh", lib.getHarvestProtocol());
        assertEquals(BigDecimal.ONE, lib.getId());
        assertEquals(null, lib.getLastHarvest());
        assertEquals("rd", lib.getMetadataFormat());
        assertEquals(null, lib.getQueryParameters());
        assertEquals("LIB1", lib.getDListValue());
    }

    @Test
    public void testUpdate() throws Exception {
        IDataSet initDataStream = support.loadFlatXmlDataStream(getClass(), "LibraryDaoTestDataSet.xml", true);
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initDataStream);

        List<Library> result = dao.find();
        assertNotNull(result);
        assertEquals(1, result.size());
        Library lib = result.get(0);
        lib.setLastHarvest("2011-04-19T15:58:29Z");
        dao.update(lib);
        transaction.commit();
        transaction.close();

        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "LibraryDaoTestUpdateResult.xml");
        IDataSet resultDS = support.getConnection().createDataSet(new String[]{"DIGKNIHOVNA"});

        System.out.println("##testUpdate");
        support.printTableAsFlatXml("DIGKNIHOVNA");
        Assertion.assertEquals(expectedDS, resultDS);
    }
}