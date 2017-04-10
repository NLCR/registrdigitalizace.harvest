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
import cz.registrdigitalizace.harvest.db.DigitizationRegistrySource;
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.LibraryDao;
import cz.registrdigitalizace.harvest.metadata.MetadataUpdater;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import cz.registrdigitalizace.harvest.oai.Harvester;
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.OaiException;
import cz.registrdigitalizace.harvest.oai.OaiSource;
import cz.registrdigitalizace.harvest.oai.OaiSourceFactory;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

/**
 * Harvests digitized objects and stores them in Digitization Registry CZ.
 *
 * TODO:
 * XXX log exceptions to DB
 *
 * @author Jan Pokorsky
 */
public final class Harvest {

    public static final String CONFIG_PROPERTY = Harvest.class.getName() + ".config";

    private static final Logger LOG = Logger.getLogger(Harvest.class.getName());
    
    private final DigitizationRegistrySource dataSource;
    private final XmlContext xmlCtx = new XmlContext();
    private final OaiSourceFactory oaiFactory;
    private final Configuration conf;
    private final Map<String, ModsMetadataParser> parserMap = new HashMap<String, ModsMetadataParser>();
    private File sessionCache;

    public Harvest() {
        this(OaiSourceFactory.getInstance());
    }

    Harvest(OaiSourceFactory oaiFactory) {
        this(oaiFactory, new Configuration());
    }

    Harvest(OaiSourceFactory oaiFactory, Configuration conf) {
        this.oaiFactory = oaiFactory;
        this.dataSource = new DigitizationRegistrySource(conf.getProperties());
        this.conf = conf;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = Configuration.fromCmdLine(args);
            if (!conf.getErrors().isEmpty()) {
                String errMsg = Utils.toString(conf.getErrors(), "\n");
                System.out.println(errMsg);
                System.out.println();
                System.out.println(Configuration.help());
                throw new IllegalStateException(errMsg);
            } else if (conf.isVersion()) {
                String implementationVersion = Harvest.class.getPackage().getImplementationVersion();
                System.out.println("harvest, " + implementationVersion);
                return ;
            } else if (conf.isHelp()) {
                System.out.println(Configuration.help());
                return ;
            }
            conf.loadConfigFile();
            Harvest harvest = new Harvest(OaiSourceFactory.getInstance(), conf);
            
            if (conf.isRegenerateMods()) {
                harvest.updateMetadata();
            } else if (conf.isUpdateThumbnails()) {
                harvest.updateThumbnails();
            } else {
                harvest.harvest();
            }
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, "Cannot start harvest process", ex);
            System.exit(1);
        }
    }

    /**
     * Harvests meta data descriptors from digital libraries, computes selected
     * meta data and updates thumbnails.
     */
    public void harvest() throws DaoException, IOException {
        this.sessionCache = null;
        List<Library> libraries = fetchLibraries();
        for (Library library : libraries) {
            harvestLibraryAndLog(library);
        }

        if (conf.isDryRun() || conf.isHarvestToCache() || conf.isNoThumbnails()) {
            return;
        }

/* dočasně zablokováno Marek - Thumbnaily se dočasně vytvářet nebudou
        ThumbnailHarvest thumbnailHarvest = new ThumbnailHarvest(dataSource, libraries);
        long time = System.currentTimeMillis();
        thumbnailHarvest.harvestThumbnails();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Thumbnail harvest status:\n  Thumbnails downloaded: {0}\n  Total size: {1} bytes\n  Time: {2}\n",
                new Object[]{thumbnailHarvest.getTotalNumber(), thumbnailHarvest.getTotalSize(), Utils.elapsedTime(time)});
*/
    }

    /**
     * Removes all selected meta data and computes them again from persisted XML descriptors.
     * <p>No harvest is run.
     */
    public void updateMetadata() throws DaoException {
        long time = System.currentTimeMillis();
        List<Library> libraries = fetchLibraries();
        MetadataUpdater mu = new MetadataUpdater(dataSource);
        for (Library library : libraries) {
            if (includeLibrary(library)) {
                mu.regenerateDigObjects(library, createMetadataParser(library));
            }
        }
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "MODS regeneration status:\n  objects updated: {0}\n  Total size: {1} bytes\n  Time: {2}\n",
                new Object[]{mu.getTotalNumber(), mu.getTotalSize(), Utils.elapsedTime(time)});
    }

    /**
     * Removes thumbnail of deleted digital objects and fetches missing.
     * <p>No harvest is run.
     */
    public void updateThumbnails() throws DaoException, IOException {
        List<Library> libraries = fetchLibraries();
        ThumbnailHarvest thumbnailHarvest = new ThumbnailHarvest(dataSource, libraries);
        long time = System.currentTimeMillis();
        thumbnailHarvest.harvestThumbnails();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Thumbnail harvest status:\n  Thumbnails downloaded: {0}\n  Total size: {1} bytes\n  Time: {2}\n",
                new Object[]{thumbnailHarvest.getTotalNumber(), thumbnailHarvest.getTotalSize(), Utils.elapsedTime(time)});
    }

    private List<Library> fetchLibraries() throws DaoException {
        LibraryDao libraryDao = new LibraryDao();
        HarvestTransaction transaction = new HarvestTransaction(dataSource);
        libraryDao.setDataSource(transaction);
        try {
            transaction.begin();
            // dočasná změna Marek - pro ostrý chod odstranit zaznam library
//            List<Library> libraries = libraryDao.find();
            List<Library> libraries = new ArrayList<Library>();
            Library library = new Library();
/*
            library.setId(new BigDecimal("463"));
            library.setHarvestProtocol("oaipmh");
            library.setMetadataFormat("drkramerius4");
            library.setLastHarvest(null);
            library.setQueryParameters(null);
            library.setBaseUrl("http://kramerius.lib.cas.cz/oaiprovider/");
            library.setDListValue("ABA007-DK");
            library.setFromDate("2017-01-01T00:00:00Z");
            library.setToDate("2017-01-10T00:00:00Z");
            libraries.add(library);
*/
//            library = new Library();
            library.setId(new BigDecimal("460"));
            library.setHarvestProtocol("oaipmh");
            library.setMetadataFormat("drkramerius4");
            library.setLastHarvest(null);
            library.setQueryParameters(null);
            library.setBaseUrl("http://kramerius4.nkp.cz/oaiprovider/");
            library.setDListValue("ABA001-DK");
//            library.setFromDate("2000-01-01T00:00:00Z");
            //dočasně//library.setFromDate("2016-04-06T00:00:00Z"); // library.setFromDate("2016-09-15T00:00:00Z"); // datum vynechano - chyba v datech
//            library.setFromDate("2017-03-04T00:00:00Z");
            //dočasně//library.setToDate("2017-06-04T00:00:00Z");
//            library.setFromDate("2014-10-01T00:00:00Z");
//            library.setToDate("2014-11-01T00:00:00Z");

            library.setFromDate("2016-09-15T22:51:00Z");
            library.setToDate("2016-09-16T00:00:00Z");

            libraries.add(library);

            // konec dočasné změny
            return libraries;
        } finally {
            transaction.close();
        }
    }

    private void harvestLibraryAndLog(Library library) {
        try {
            harvestLibrary(library);
        } catch (Exception ex) {
            // catch everything not to break other libraries processing
            LOG.log(Level.SEVERE, "  chyba: ", ex);
        }
    }

    private boolean includeLibrary(Library library) {
        if (conf.getIncludeLibraries().isEmpty()) {
            if (conf.getExcludeLibraries().contains(library.getId())) {
                return false;
            }
        } else {
            if (!conf.getIncludeLibraries().contains(library.getId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Harvests library meta data and persists them in DB.
     * <p>The persistence is optional.
     */
    private void harvestLibrary(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
        if (!includeLibrary(library)) {
            return ;
        }
        Library modifiedLibrary;

        String datumOdLocalStr = library.getFromDate();
        Date datumOdLocal;
        Date datumDoLocal;
        Date datumZacatek;
        Date datumPomocny;
        Date datumDnesek = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
        /* // max datum ke kterému jsou záznamy načítány
        Date datumDnesek = null;
        try {
            datumDnesek = simpleDateFormat2.parse("2017-03-10");
        } catch (ParseException ex) {
            
        }
        */
        Calendar c = Calendar.getInstance();
        Boolean pokracuj = true;
        try {
            datumDnesek = simpleDateFormat2.parse(simpleDateFormat2.format(datumDnesek));
            c.setTime(simpleDateFormat.parse(datumOdLocalStr));
            datumOdLocal = c.getTime();
        } catch (ParseException ex) {
            try {
                c.setTime(simpleDateFormat2.parse(datumOdLocalStr));
                datumOdLocal = c.getTime();
            } catch (ParseException ex2) {
                System.out.println("Exception when set toDate: "+ex2);
                pokracuj = false;
                datumOdLocal = null;
            }
        }
        if ((pokracuj) && (datumOdLocal != null)) {
            modifiedLibrary = library;
            datumZacatek = datumOdLocal;
            pokracuj = true;
            while ((datumOdLocal.before(datumDnesek)) && (pokracuj)) {
                if (datumZacatek.equals(datumOdLocal)) {
                    try {
                        datumPomocny = simpleDateFormat.parse(simpleDateFormat2.format(datumOdLocal) + "T00:00:00Z");
                    } catch (ParseException ex) {
                        pokracuj = false;
                        datumPomocny = null;
                    }
                } else {
                    datumPomocny = datumOdLocal;
                }
                if ((pokracuj) && (datumPomocny != null)) {
                    c.setTime(datumPomocny);
                    c.add(Calendar.DAY_OF_MONTH, 1);
                    datumDoLocal =  c.getTime();
                    modifiedLibrary.setToDate(simpleDateFormat.format(datumDoLocal));

                    System.out.println("  rozmeziDatumu: " + modifiedLibrary.getFromDate() + " - " + modifiedLibrary.getToDate());

                    Boolean nacteno = false;
                    int pocitadloPokusu = 0;
                    while ((!nacteno) && (pocitadloPokusu < 5)) {
                        try {
                            harvestLibraryOneDay(modifiedLibrary);
                            nacteno = true;
                        } catch (cz.registrdigitalizace.harvest.oai.OaiException ex) {
                            pocitadloPokusu++;
                            System.out.println(" pokus cislo: " + pocitadloPokusu + " " + ex.getMessage());
                            LOG.log(Level.INFO, (" pokus cislo: " + pocitadloPokusu));
                        }
                    }
                    if (!nacteno) {
                        pokracuj = false;
                    }
                    datumOdLocal =  datumDoLocal;
                    modifiedLibrary.setFromDate(simpleDateFormat.format(datumDoLocal));
                }

            }
            if (!pokracuj) {
                System.out.println("  zpracovani preruseno pro timeout");
            }
            //System.out.println("  jsem za nactenim zaznamu pro jednotlive datumy");
            
        }

    }

    private void harvestLibraryOneDay(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
        long time = System.currentTimeMillis();
        String casPosledniZpracovanyZaznam = "";

        OaiSource oaiSource = resolveOaiSource(library);
        if (oaiSource == null) {
            LOG.log(Level.FINE, "Skip {0}", library);
            return ;
        }
        LOG.log(Level.INFO, "Harvesting {0}", library);
        Harvester harvester = new Harvester(oaiSource, xmlCtx);
        ListResult<Record> oaiRecords = harvester.getListRecords(true);
        try {
            if (conf.isHarvestToCache()) {
                cacheRecords(library, oaiRecords, time);
            } else {
                if (conf.isHarvestWithCache()) {
                    cacheRecords(library, oaiRecords, time);
                    oaiRecords.close();
                    oaiSource = oaiFactory.createSourceFromCache(library.getCacheFolder());
                    harvester = new Harvester(oaiSource, xmlCtx);
                    oaiRecords = harvester.getListRecords(true);
                }
                casPosledniZpracovanyZaznam = persistRecords(library, oaiRecords, time);
                if (!"".equals(casPosledniZpracovanyZaznam)) {
System.out.println("  zkusim posunout cas a spustit znovu");
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                    SimpleDateFormat simpleDF = new SimpleDateFormat("yyyyMMdd");
                    Date datumDnesek = new Date();
                    Date datumPomocny;
                    Date datumFromLocal;
                    Calendar c = Calendar.getInstance();
                    Library libraryLocal = library;

                    while (!"".equals(casPosledniZpracovanyZaznam)) {
                        try {
                            datumPomocny = simpleDateFormat.parse(casPosledniZpracovanyZaznam);
                            c.setTime(datumPomocny);
                            c.add(Calendar.SECOND, 1);
                            datumFromLocal =  c.getTime();
                            libraryLocal.setFromDate(simpleDateFormat.format(datumFromLocal));
//System.out.println("  nastavuji novy cas zacatku na: " + libraryLocal.getFromDate() + " - " + libraryLocal.getToDate());

                            OaiSource oaiSourceLocal = resolveOaiSource(library);
                            Harvester harvesterLocal = new Harvester(oaiSourceLocal, xmlCtx);
                            ListResult<Record> oaiRecordsLocal = harvester.getListRecords(true);
                            casPosledniZpracovanyZaznam = persistRecords(libraryLocal, harvesterLocal.getListRecords(true), System.currentTimeMillis());
                            oaiRecordsLocal.close();
                        } catch (ParseException ex) {
                            String fname= "." + File.separator + libraryLocal.getDListValue() + "-" + simpleDF.format(datumDnesek);
                            BufferedWriter bw = null;
                            FileWriter fw = null;
                            try {
                                File file = new File(fname);
                                if (!file.exists()) {
                                        file.createNewFile();
                                }
                                fw = new FileWriter(file.getAbsoluteFile(), true);
                                bw = new BufferedWriter(fw);
                                bw.write("nezpracovane zaznamy od: " + casPosledniZpracovanyZaznam + " do: " + library.getToDate());
                            } catch (IOException e) {
                            } finally {
                                try {
                                    if (bw != null)
                                            bw.close();
                                    if (fw != null)
                                            fw.close();
                                } catch (IOException exClose) {
                                }
                            }
                            
                        }
                        //casPosledniZpracovanyZaznam = persistRecords(library, oaiRecords, time);
                    }
                }
            }
        } finally {
            oaiRecords.close();
        }

    }

    /** Iterates records to get them persisted */
    private String persistRecords(Library library, ListResult<Record> oaiRecords, long time)
            throws DaoException, JAXBException, XMLStreamException {
        String casPosledniZpracovanyZaznam = "";

        LibraryHarvest libraryHarvest = new LibraryHarvest(library, dataSource,
                createMetadataParser(library), conf.isDryRun());
        System.out.println("  spoustim libraryHarvest");
        casPosledniZpracovanyZaznam = libraryHarvest.harvest(oaiRecords, xmlCtx);
        System.out.println("  ukoncuji libraryHarvest");
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Harvest status:\n  Records added: {0}\n  Records deleted: {1}\n  Time: {2}\n",
                new Object[]{libraryHarvest.getAddRecordCount(), libraryHarvest.getRemoveRecordCount(), Utils.elapsedTime(time)});
        return casPosledniZpracovanyZaznam;
    }

    private ModsMetadataParser createMetadataParser(Library library) {
        String xslt = conf.getMetadataXslt(library.getDListValue());
        if (xslt != null) {
//            xslt = new File(xslt).toURI().toASCIIString();
        } else {
            xslt = ModsMetadataParser.getDefaultXslt();
        }
        return createMetadataParser(xslt);
    }

    private ModsMetadataParser createMetadataParser(String xslt) {
        ModsMetadataParser parser = parserMap.get(xslt);
        if (parser == null) {
            parser = new ModsMetadataParser(xslt);
            parserMap.put(xslt, parser);
        }
        return parser;
    }

    /** Iterates records to get them cached */
    private void cacheRecords(Library library, ListResult<Record> oaiRecords, long time) {
        int count = 0;
        for (Record record : oaiRecords) {
            ++count;
        }
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Harvest status:\n  Records cached: {0}\n  Time: {1}\n  Cache: {2}\n",
                new Object[]{count, Utils.elapsedTime(time), library.getCacheFolder()});
    }

    /**
     * Folder to store given library harvest from this session.
     */
    private File createCacheFolder(Library library) throws FileNotFoundException {
        File cache = new File(getCacheFolder(), String.valueOf(library.getId()));
        cache.mkdirs();
        return cache;
    }

    /**
     * Folder to store all harvests from this session.
     */
    private File getCacheFolder() throws FileNotFoundException {
        if (sessionCache == null) {
            String session = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            sessionCache = new File(getCacheRootFolder(), session);
            sessionCache.mkdir();
        }
        return sessionCache;
    }

    /**
     * Folder to store all harvests.
     */
    private File getCacheRootFolder() throws FileNotFoundException {
        String cachePath = conf.getCacheRoot();
        File cache = new File(cachePath);
        cache.mkdirs();
        if (!cache.exists() || !cache.isDirectory()) {
            throw new FileNotFoundException(cache.toString());
        }
        return cache;
    }

    private OaiSource resolveOaiSource(Library library) throws IOException {
        if (conf.isHarvestFromCache()) {
            String cachePath = conf.getCachePath();
            File cache = new File(cachePath);
            File libraryCache = new File(cache, String.valueOf(library.getId()));
            if (!libraryCache.exists()) {
                LOG.log(Level.WARNING, "Missing library cache: {0}", libraryCache);
                return null;
            }
            return oaiFactory.createSourceFromCache(libraryCache);
        }
        String validate = library.validate();
        if (validate != null) {
            LOG.log(Level.FINE, "{1}: {0}", new Object[]{validate, library});
            return null;
        }
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                OaiSource src = oaiFactory.createListRecords(
                        library.getBaseUrl(), library.getLastHarvest(), library.getFromDate(), library.getToDate(),
                        library.getMetadataFormat(), library.getQueryParameters());
                if (conf.isHarvestToCache() || conf.isHarvestWithCache()) {
                    File cache = createCacheFolder(library);
                    src = oaiFactory.createCachedSource(src, cache);
                    library.setCacheFolder(cache);
                }
                return src;
            } catch (OaiException ex) {
                LOG.log(Level.WARNING, "Invalid OAI URL ''{0}''\n  {1}",
                        new Object[]{ex.getLocalizedMessage(), library});
                return null;
            }
        }
        return null;
    }

    protected class InterruptTimerTask extends TimerTask {

        private Thread theTread;

        public InterruptTimerTask(Thread theTread) {
            this.theTread = theTread;
        }

        @Override
        public void run() {
            theTread.interrupt();
        }

    }

}
