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
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.IdSequenceDao;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.LibraryDao;
import cz.registrdigitalizace.harvest.db.MetadataDao;
import cz.registrdigitalizace.harvest.db.RecordRepository;
import cz.registrdigitalizace.harvest.db.RelationDao;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.MetadataParser;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.RecordTypeParser;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import org.openarchives.oai2.HeaderType;
import org.openarchives.oai2.StatusType;

/**
 * Parses harvested OAI records and persists them together with the time
 * of the harvest.
 *
 * @author Jan Pokorsky
 */
public class LibraryHarvest {
    private static final Logger LOG = Logger.getLogger(LibraryHarvest.class.getName());
    
    private final ModsMetadataParser modsParser;
    private final Library library;
    private final HarvestTransaction transaction;
    private final IdSequenceDao idSequenceDao = new IdSequenceDao();
    private final DigObjectDao digiObjectDao = new DigObjectDao();
    private final RelationDao relationDao = new RelationDao();
    private final LibraryDao libraryDao = new LibraryDao();
    private final MetadataDao metadataDao = new MetadataDao();
    private int addCounter;
    private int removeCounter;
    private final boolean dryRun;

    /**
     * 
     * @param library (Library)
     * @param source (DigitizationRegistrySource)
     * @param metadataParser (ModsMetadataParser)
     * @param dryRun (boolean)
     */
    public LibraryHarvest(Library library, DigitizationRegistrySource source,
            ModsMetadataParser metadataParser, boolean dryRun) {
        this.library = library;
        this.transaction = new HarvestTransaction(source);
        this.modsParser = metadataParser;
        this.dryRun = dryRun;
    }

    /**
     * zpracovává načtené záznamy v rámci transakce
     * @param listRecords (ListResult<Record>)
     * @param xmlCtx (XmlContext)
     * @return (String)
     * @throws DaoException
     * @throws JAXBException
     * @throws XMLStreamException 
     */
    public String harvest(ListResult<Record> listRecords, XmlContext xmlCtx) throws DaoException, JAXBException, XMLStreamException {
        String casChyby = "";
        String casPosledniZpracovanyZaznam;
        transaction.begin();
        boolean rollback = true;
        try {
            casPosledniZpracovanyZaznam = persistRecords(listRecords.iterator(), xmlCtx);
            if (!"OK".equals(casPosledniZpracovanyZaznam)) {
                LOG.log(Level.SEVERE, " chyba pri behu: " + casPosledniZpracovanyZaznam);
                System.out.println(" chyba pri behu: " + casPosledniZpracovanyZaznam);
            } else {
                casPosledniZpracovanyZaznam = "";
            }
            libraryDao.setDataSource(transaction);
            String harvestDate = listRecords.getResponseDate().toString();
            library.setLastHarvest(harvestDate);
            libraryDao.update(library);

            if (!dryRun) {
                transaction.commit();
                rollback = false;
            }
        } finally {
            System.out.println("  ukoncuji transakci");
            if (rollback) {
                transaction.rollback();
            }
            transaction.close();
        }
        return casPosledniZpracovanyZaznam;
    }

    /**
     * zpracuje načtené záznamy
     * @param recordIterator (Iterator<Record>)
     * @param xmlCtx (XmlContext)
     * @return (String)
     * @throws DaoException
     * @throws JAXBException 
     */
    private String persistRecords(Iterator<Record> recordIterator, XmlContext xmlCtx)
            throws DaoException, JAXBException/*, XMLStreamException*/ {
        String timestampPosledniZpracovanyZaznam = "";
        
        System.out.println("    spoustim persistRecord");
        if (!recordIterator.hasNext()) {
            System.out.println("    nejsou zaznamy, ukoncuji persistRecord");
            return "OK";
        }
        RecordRepository processor = createProcessor();
        System.out.println("  procesorVytvoren: ");
        //dočasně blokováno// processor.init();
        System.out.println("  procesorInicializovan: ");
        int pocetZpracovanychZaznamu = 0;

        try {
            while (recordIterator.hasNext()) {
                pocetZpracovanychZaznamu++;
                //System.out.println(" zpracovavamZaznam: " + pocetZpracovanychZaznamu);
                Record oairecord = recordIterator.next();
                RecordTypeParser parser = oairecord.getParser();
                HeaderType headerType = parser.parseHeader();
                if (headerType.getStatus() == StatusType.DELETED) {
                    HarvestedRecord record = resolveDeleteRecord(headerType.getIdentifier());
                    //dočasně blokováno// processor.remove(record); // docasne zablokovano
                    removeCounter++;
                } else {
                    HarvestedRecord metadata = parser.parseMetadata(new KrameriusParser(xmlCtx, modsParser));
                    //dočasně blokováno// processor.add(metadata); // docasne zablokovano
                    addCounter++;
                }
                timestampPosledniZpracovanyZaznam = headerType.getDatestamp();
                System.out.println("  zaznam: " + pocetZpracovanychZaznamu + " zpracovan");
            }
        } catch (Exception ex) {
            System.out.println("  chyba pri zpracovani XML: " + ex + " -- " + timestampPosledniZpracovanyZaznam);
            LOG.log(Level.SEVERE, "  chyba pri zpracovani XML: " + ex + " -- " + timestampPosledniZpracovanyZaznam);
            return timestampPosledniZpracovanyZaznam; // poslat cas posledniho spravneho
        }
        System.out.println("  uzaviram zpracovani zaznamu (processor.close)");
        //dočasně blokováno// processor.close(); // docasne zablokovano
        System.out.println("    ukoncuji persistRecord");
        return "OK";
    }

    /**
     * 
     * @return (int)
     */
    public int getAddRecordCount() {
        return addCounter;
    }

    /**
     * 
     * @return (int)
     */
    public int getRemoveRecordCount() {
        return removeCounter;
    }

    /**
     * 
     * @param identifier (String)
     * @return 
     */
    private HarvestedRecord resolveDeleteRecord(String identifier) {
        // XXX make UUID parse more robust
        int lastIndexOf = identifier.lastIndexOf(':');
        String uuid = (lastIndexOf > 0) ? identifier.substring(lastIndexOf) : identifier;
        HarvestedRecord rec = new HarvestedRecord();
        rec.setUuid(uuid);
        return rec;
    }

    /**
     * vytvoření procesoru
     * @return (RecordRepository)
     * @throws DaoException 
     */
    private RecordRepository createProcessor() throws DaoException {
        idSequenceDao.setDataSource(transaction);
        relationDao.setDataSource(transaction);
        digiObjectDao.setDataSource(transaction);
        metadataDao.setDataSource(transaction);

        return new RecordRepository(digiObjectDao, relationDao,
                idSequenceDao, metadataDao, library);
    }

}
