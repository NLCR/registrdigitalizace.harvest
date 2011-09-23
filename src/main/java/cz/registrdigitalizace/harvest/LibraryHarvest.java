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

package cz.registrdigitalizace.harvest;

import cz.registrdigitalizace.harvest.db.DaoException;
import cz.registrdigitalizace.harvest.db.DigObjectDao;
import cz.registrdigitalizace.harvest.db.DigitizationRegistrySource;
import cz.registrdigitalizace.harvest.db.RecordRepository;
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.IdSequenceDao;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.LibraryDao;
import cz.registrdigitalizace.harvest.db.LocationDao;
import cz.registrdigitalizace.harvest.db.MetadataDao;
import cz.registrdigitalizace.harvest.db.RelationDao;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.RecordTypeParser;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.openarchives.oai2.HeaderType;
import org.openarchives.oai2.StatusType;

/**
 *
 * Parses harvested OAI records and persists them together with the time
 * of the harvest.
 *
 * @author Jan Pokorsky
 */
public class LibraryHarvest {
    // parser will be configurable per library at some time in future
    private static final ModsMetadataParser modsParser = new ModsMetadataParser(ModsMetadataParser.MZK_STYLESHEET);
    private final Library library;
    private final HarvestTransaction transaction;
    private final IdSequenceDao idSequenceDao = new IdSequenceDao();
    private final LocationDao locationDao = new LocationDao();
    private final DigObjectDao digiObjectDao = new DigObjectDao();
    private final RelationDao relationDao = new RelationDao();
    private final LibraryDao libraryDao = new LibraryDao();
    private final MetadataDao metadataDao = new MetadataDao();
    private int addCounter;
    private int removeCounter;

    public LibraryHarvest(Library library, DigitizationRegistrySource source) {
        this.library = library;
        this.transaction = new HarvestTransaction(source);
    }

    public void harvest(ListResult<Record> listRecords, XmlContext xmlCtx) throws DaoException, JAXBException, XMLStreamException {
        transaction.begin();
        boolean rollback = true;
        try {
            libraryDao.setDataSource(transaction);
            RecordRepository processor = createProcessor();
            processor.init();

            String harvestDate = listRecords.getResponseDate().toString();
            library.setLastHarvest(harvestDate);
            for (Record oairecord : listRecords) {
                RecordTypeParser parser = oairecord.getParser();
                HeaderType headerType = parser.parseHeader();
                if (headerType.getStatus() == StatusType.DELETED) {
                    HarvestedRecord record = resolveDeleteRecord(headerType.getIdentifier());
                    processor.remove(record);
                    removeCounter++;
                } else {
                    HarvestedRecord metadata = parser.parseMetadata(new KrameriusParser(xmlCtx, modsParser));
                    processor.add(metadata);
                    // parser.parseAbouts();
                    addCounter++;
                }
            }

            processor.close();

            libraryDao.update(library);

            transaction.commit();
            rollback = false;
        } finally {
            if (rollback) {
                transaction.rollback();
            }
            transaction.close();
        }

    }

    public int getAddRecordCount() {
        return addCounter;
    }

    public int getRemoveRecordCount() {
        return removeCounter;
    }

    private HarvestedRecord resolveDeleteRecord(String identifier) {
        // XXX make UUID parse more robust
        int lastIndexOf = identifier.lastIndexOf(':');
        if (lastIndexOf > 0) {

        }
        String uuid = (lastIndexOf > 0) ? identifier.substring(lastIndexOf) : identifier;
        HarvestedRecord rec = new HarvestedRecord();
        rec.setUuid(uuid);
        return rec;
    }

    private RecordRepository createProcessor() throws DaoException {
        idSequenceDao.setDataSource(transaction);
        locationDao.setDataSource(transaction);
        relationDao.setDataSource(transaction);
        digiObjectDao.setDataSource(transaction);
        metadataDao.setDataSource(transaction);

        return new RecordRepository(locationDao, digiObjectDao, relationDao,
                idSequenceDao, metadataDao, library);
    }

}
