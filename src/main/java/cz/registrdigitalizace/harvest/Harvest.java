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
import cz.registrdigitalizace.harvest.oai.Harvester;
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.OaiException;
import cz.registrdigitalizace.harvest.oai.OaiSource;
import cz.registrdigitalizace.harvest.oai.OaiSourceFactory;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
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
    private File sessionCache;

    public Harvest() {
        this(OaiSourceFactory.getInstance());
    }

    Harvest(OaiSourceFactory oaiFactory) {
        this(oaiFactory, new Configuration());
    }

    Harvest(OaiSourceFactory oaiFactory, Configuration conf) {
        this.oaiFactory = oaiFactory;
        this.dataSource = new DigitizationRegistrySource(resolveConfig());
        this.conf = conf;
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            Configuration conf = Configuration.fromCmdLine(args);
            if (conf.isVersion()) {
                String implementationVersion = Harvest.class.getPackage().getImplementationVersion();
                System.out.println("harvest, " + implementationVersion);
                return ;
            } else if (conf.isHelp()) {
                System.out.println(Configuration.help());
                return ;
            }
            Harvest harvest = new Harvest(OaiSourceFactory.getInstance(), conf);
            
            if (conf.isRegenerateMods()) {
                harvest.updateMetadata();
            } else {
                harvest.harvest();
            }
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, "Cannot start harvest process", ex);
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

        if (conf.isHarvestToCache()) {
            return;
        }
        computeMetadata();
        
        ThumbnailHarvest thumbnailHarvest = new ThumbnailHarvest(dataSource, libraries);
        long time = System.currentTimeMillis();
        thumbnailHarvest.harvestThumbnails();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Thumbnail harvest status:\n  Thumbnails downloaded: {0}\n  Total size: {1} bytes\n  Time: {2} ms\n",
                new Object[]{thumbnailHarvest.getTotalNumber(), thumbnailHarvest.getTotalSize(), Utils.elapsedTime(time)});
    }

    /**
     * Removes all selected meta data and computes them again from persisted XML descriptors.
     * <p>No harvest is run.
     */
    public void updateMetadata() throws DaoException {
        MetadataUpdater mu = new MetadataUpdater(dataSource);
        long time = System.currentTimeMillis();
        mu.regenerateDigObjects();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "MODS regeneration status:\n  objects updated: {0}\n  Total size: {1} bytes\n  Time: {2} ms\n",
                new Object[]{mu.getTotalNumber(), mu.getTotalSize(), Utils.elapsedTime(time)});
    }

    /**
     * Computes selected meta data from just harvested and persisted XML descriptors.
     */
    private void computeMetadata() throws DaoException {
        MetadataUpdater mu = new MetadataUpdater(dataSource);
        long time = System.currentTimeMillis();
        mu.generateModifiedDigObjects();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "MODS generation status:\n  objects updated: {0}\n  Time: {0} ms\n",
                new Object[]{mu.getTotalNumber(), Utils.elapsedTime(time)});
    }

    private List<Library> fetchLibraries() throws DaoException {
        LibraryDao libraryDao = new LibraryDao();
        HarvestTransaction transaction = new HarvestTransaction(dataSource);
        libraryDao.setDataSource(transaction);
        try {
            transaction.begin();
            List<Library> libraries = libraryDao.find();
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
            LOG.log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Harvests library meta data and persists them in DB.
     * <p>The persistence is optional.
     */
    private void harvestLibrary(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
        LOG.log(Level.INFO, "Harvesting {0}", library);
        long time = System.currentTimeMillis();
        OaiSource oaiSource = resolveOaiSource(library);
        if (oaiSource == null) {
            return ;
        }
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
                persistRecords(library, oaiRecords, time);
            }
        } finally {
            oaiRecords.close();
        }
    }

    /** Iterates records to get them persisted */
    private void persistRecords(Library library, ListResult<Record> oaiRecords, long time)
            throws DaoException, JAXBException, XMLStreamException {

        LibraryHarvest libraryHarvest = new LibraryHarvest(library, dataSource);
        libraryHarvest.harvest(oaiRecords, xmlCtx);
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Harvest status:\n  Records added: {0}\n  Records deleted: {1}\n  Time: {2} ms\n",
                new Object[]{libraryHarvest.getAddRecordCount(), libraryHarvest.getRemoveRecordCount(), time});
    }

    /** Iterates records to get them cached */
    private void cacheRecords(Library library, ListResult<Record> oaiRecords, long time) {
        int count = 0;
        for (Record record : oaiRecords) {
            ++count;
        }
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Harvest status:\n  Records cached: {0}\n  Time: {1} ms\n  Cache: {2}\n",
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
            LOG.log(Level.WARNING, "{1}: {0}", new Object[]{validate, library});
            return null;
        }
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                OaiSource src = oaiFactory.createListRecords(
                        library.getBaseUrl(), library.getLastHarvest(),
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
        } else {
            LOG.log(Level.WARNING, "Unknown protocol ''{0}''\n  {1}",
                    new Object[]{library.getHarvestProtocol(), library});
        }
        return null;
    }

    /**
     * Fetches properties form file specified as
     * {@code -Dcz.registrdigitalizace.harvest.Harvest.config}.
     * 
     * @return fetched properties or system properties
     */
    private static Properties resolveConfig() {
        Properties properties = System.getProperties();
        String configPath = properties.getProperty(CONFIG_PROPERTY);
        LOG.log(Level.FINE, "CONFIG_PROPERTY: {0}", configPath);
        if (configPath != null) {
            InputStreamReader reader = null;
            try {
                reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8");
                properties = new Properties();
                properties.load(reader);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, null, ex);
            } finally {
                try {
                    reader.close();
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, null, ex);
                }
            }
            if (reader == null) {
                System.exit(1);
            }
        }
        LOG.log(Level.FINE, "config: {0}", properties.toString());
        return properties;
    }

}
