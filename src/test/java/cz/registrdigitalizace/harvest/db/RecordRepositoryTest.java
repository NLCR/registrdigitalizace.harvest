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

import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.DefaultDataSet;
import cz.registrdigitalizace.harvest.HarvestedRecord;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.dbunit.Assertion;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ReplacementDataSet;
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
public class RecordRepositoryTest {
    private DbUnitSupport support;
    private HarvestTransaction transaction;
    private LocationDao locationDao;
    private DigObjectDao digiObjectDao;
    private RelationDao relationDao;
    private IdSequenceDao idSequenceDao;
    private LibraryDao libraryDao;

    public RecordRepositoryTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws SQLException {
        support = new DbUnitSupport();
        transaction = new HarvestTransaction(support.getSource());
        locationDao = new LocationDao();
        locationDao.setDataSource(transaction);
        digiObjectDao = new DigObjectDao();
        digiObjectDao.setDataSource(transaction);
        relationDao = new RelationDao();
        relationDao.setDataSource(transaction);
        idSequenceDao = new IdSequenceDao();
        idSequenceDao.setDataSource(transaction);
        libraryDao = new LibraryDao();
        libraryDao.setDataSource(transaction);
    }

    @After
    public void tearDown() throws Exception {
        if (support != null) {
            support.close();
        }
    }

    @Test
    public void testAdd() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "RecordRepositoryTestAddDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        HashMap<String, String> replacements = new HashMap<String, String>();
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "RecordRepositoryTestAddResult.xml");
        replacements.put("${currentDate}", new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        expectedDS = new ReplacementDataSet(expectedDS, replacements, null);

        HarvestedRecord periodical = DigObjectDaoTest.createNewRecord("uuid:2", true, "periodical", "Periodical", "uuid:3");
        HarvestedRecord volume = DigObjectDaoTest.createNewRecord("uuid:3", false, "volume", "Volume", "uuid:4");
        HarvestedRecord issue = DigObjectDaoTest.createNewRecord("uuid:4", false, "issue", "Issue 1");

        transaction.begin();
        try {
            try {
                List<Library> libraries = libraryDao.find();
                assertNotNull(libraries);
                assertEquals(1, libraries.size());
                Library library = libraries.get(0);

                RecordRepository instance = new RecordRepository(
                        locationDao, digiObjectDao, relationDao,
                        idSequenceDao, library);
                instance.init();
                instance.add(periodical);
                instance.add(volume);
                instance.add(issue);
                instance.close();
                transaction.commit();
            } catch (Throwable t) {
                transaction.rollback();
                throw new IllegalStateException(t);
            }
        } finally {
            transaction.close();
        }

        IDataSet resultDS = support.getConnection().createDataSet();
        support.dumpTable(expectedDS.getTable("PLAANT_IDS"));
        support.dumpTable(resultDS.getTable("PLAANT_IDS"));
////        support.printTableAsFlatXml("LOKACE");
        Assertion.assertEquals(expectedDS.getTable("DIGOBJEKT"), resultDS.getTable("DIGOBJEKT"));
        Assertion.assertEqualsIgnoreCols(expectedDS.getTable("LOKACE"), resultDS.getTable("LOKACE"), new String[] {"XML"});
        Assertion.assertEquals(expectedDS.getTable("DIGVAZBY"), resultDS.getTable("DIGVAZBY"));
        support.dumpTable(new SortedTable(expectedDS.getTable("PLAANT_IDS")));
        support.dumpTable(new SortedTable(resultDS.getTable("PLAANT_IDS")));
        Assertion.assertEquals(new SortedTable(expectedDS.getTable("PLAANT_IDS")),
                new SortedTable(resultDS.getTable("PLAANT_IDS")));
    }

    @Test
    public void testRemove() throws Exception {
        IDataSet initialDS = support.loadFlatXmlDataStream(getClass(), "RecordRepositoryTestRemoveDataSet.xml");
        DatabaseOperation.CLEAN_INSERT.execute(support.getConnection(), initialDS);
        IDataSet expectedDS = support.loadFlatXmlDataStream(getClass(), "RecordRepositoryTestRemoveResult.xml");

        HarvestedRecord periodical = DigObjectDaoTest.createNewRecord("uuid:2", true, "periodical", "Periodical", "uuid:3");
        HarvestedRecord volume = DigObjectDaoTest.createNewRecord("uuid:3", false, "volume", "Volume", "uuid:4");
        HarvestedRecord issue = DigObjectDaoTest.createNewRecord("uuid:4", false, "issue", "Issue 1");

        transaction.begin();
        try {
            try {
                List<Library> libraries = libraryDao.find();
                assertNotNull(libraries);
                assertEquals(1, libraries.size());
                Library library = libraries.get(0);

                RecordRepository builder = new RecordRepository(
                        locationDao, digiObjectDao, relationDao,
                        idSequenceDao, library);
                builder.init();
                builder.remove(periodical);
                builder.remove(issue);
                builder.remove(volume);
                builder.close();
                transaction.commit();
            } catch (Throwable t) {
                transaction.rollback();
                throw new IllegalStateException(t);
            }
        } finally {
            transaction.close();
        }

        IDataSet resultDS = support.getConnection().createDataSet();

        Assertion.assertEquals(expectedDS.getTable("DIGOBJEKT"), resultDS.getTable("DIGOBJEKT"));
        Assertion.assertEqualsIgnoreCols(expectedDS.getTable("LOKACE"), resultDS.getTable("LOKACE"), new String[] {"XML"});
        Assertion.assertEquals(expectedDS.getTable("DIGVAZBY"), resultDS.getTable("DIGVAZBY"));
        Assertion.assertEquals(new SortedTable(expectedDS.getTable("PLAANT_IDS")),
                new SortedTable(resultDS.getTable("PLAANT_IDS")));
    }

}