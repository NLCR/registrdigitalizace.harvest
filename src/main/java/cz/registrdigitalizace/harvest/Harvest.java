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
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.Harvester;
import cz.registrdigitalizace.harvest.oai.OaiException;
import cz.registrdigitalizace.harvest.oai.OaiSource;
import cz.registrdigitalizace.harvest.oai.OaiSourceFactory;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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

    public Harvest() {
        this(OaiSourceFactory.getInstance());
    }

    Harvest(OaiSourceFactory oaiFactory) {
        this.oaiFactory = oaiFactory;
        this.dataSource = new DigitizationRegistrySource(resolveConfig());
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            CmdLine cmdLine = new CmdLine(args);
            if (cmdLine.isVersion()) {
                String implementationVersion = Harvest.class.getPackage().getImplementationVersion();
                System.out.println("harvest, " + implementationVersion);
                return ;
            }
            Harvest harvest = new Harvest();
            
            if (cmdLine.isRegenerateMods()) {
                harvest.regenerateMetadata();
            } else {
                harvest.harvest();
            }
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, "Cannot start harvest process", ex);
        }
    }

    public void harvest() throws DaoException, IOException {
        List<Library> libraries = fetchLibraries();
        for (Library library : libraries) {
            harvestLibraryAndLog(library);
        }
        
        updateMetadata();
        
        ThumbnailHarvest thumbnailHarvest = new ThumbnailHarvest(dataSource, libraries);
        long time = System.currentTimeMillis();
        thumbnailHarvest.harvestThumbnails();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Thumbnail harvest status:\n  Thumbnails downloaded: {0}\n  Total size: {1} bytes\n  Time: {2} ms\n",
                new Object[]{thumbnailHarvest.getTotalNumber(), thumbnailHarvest.getTotalSize(), time});
    }

    public void regenerateMetadata() throws DaoException {
        MetadataUpdater mu = new MetadataUpdater(dataSource);
        long time = System.currentTimeMillis();
        mu.regenerateDigObjects();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "MODS regeneration status:\n  objects updated: {0}\n  Total size: {1} bytes\n  Time: {2} ms\n",
                new Object[]{mu.getTotalNumber(), mu.getTotalSize(), time});
    }

    private void updateMetadata() throws DaoException {
        MetadataUpdater mu = new MetadataUpdater(dataSource);
        long time = System.currentTimeMillis();
        mu.generateModifiedDigObjects();
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "MODS generation status:\n  objects updated: {0}\n  Time: {0} ms\n",
                new Object[]{mu.getTotalNumber(), time});
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
    private void harvestLibrary(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException {
        LOG.log(Level.INFO, "Harvesting {0}", library);
        long time = System.currentTimeMillis();
        OaiSource oaiSource = resolveOaiSource(library);
        if (oaiSource == null) {
            return ;
        }
        Harvester harvester = new Harvester(oaiSource, xmlCtx);
        ListResult<Record> oaiRecords = harvester.getListRecords(true);
        try {
            LibraryHarvest libraryHarvest = new LibraryHarvest(library, dataSource);
            libraryHarvest.harvest(oaiRecords, xmlCtx);
            time = System.currentTimeMillis() - time;
            LOG.log(Level.INFO, "Harvest status:\n  Records added: {0}\n  Records deleted: {1}\n  Time: {2} ms\n",
                    new Object[]{libraryHarvest.getAddRecordCount(), libraryHarvest.getRemoveRecordCount(), time});
        } finally {
            oaiRecords.close();
        }

    }

    private OaiSource resolveOaiSource(Library library) {
        String validate = library.validate();
        if (validate != null) {
            LOG.log(Level.WARNING, "{1}: {0}", new Object[]{validate, library});
            return null;
        }
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                return oaiFactory.createListRecords(
                        library.getBaseUrl(), library.getLastHarvest(),
                        library.getMetadataFormat(), library.getQueryParameters());
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
    
    private static final class CmdLine {
        
        private boolean regenerateMods;
        private boolean version;

        public CmdLine(String[] args) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if ("-regenerateMods".equals(arg)) {
                    this.regenerateMods = true;
                } else if ("-version".equals(arg)) {
                    this.version = true;
                }
            }
        }

        public boolean isRegenerateMods() {
            return regenerateMods;
        }

        public boolean isVersion() {
            return version;
        }
        
    }

}
