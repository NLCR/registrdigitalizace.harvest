/*
 * Copyright (C) 2017 Marek Kortus based on 2011 Jan Pokorsky
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.registrdigitalizace.harvest.db.AutorEntry;
import cz.registrdigitalizace.harvest.db.DaoException;
import cz.registrdigitalizace.harvest.db.DigitizationRegistrySource;
import cz.registrdigitalizace.harvest.db.HarvestTransaction;
import cz.registrdigitalizace.harvest.db.KrameriusEntry;
import cz.registrdigitalizace.harvest.db.Library;
import cz.registrdigitalizace.harvest.db.LibraryDao;
import cz.registrdigitalizace.harvest.db.NakladatelEntry;
import cz.registrdigitalizace.harvest.db.NazevEntry;
import cz.registrdigitalizace.harvest.metadata.MetadataUpdater;
import cz.registrdigitalizace.harvest.metadata.ModsMetadataParser;
import cz.registrdigitalizace.harvest.oai.Harvester;
import cz.registrdigitalizace.harvest.oai.HarvesterInputStream;
import cz.registrdigitalizace.harvest.oai.ListResult;
import cz.registrdigitalizace.harvest.oai.OaiException;
import cz.registrdigitalizace.harvest.oai.OaiParser;
import cz.registrdigitalizace.harvest.oai.OaiSource;
import cz.registrdigitalizace.harvest.oai.OaiSourceFactory;
import cz.registrdigitalizace.harvest.oai.Record;
import cz.registrdigitalizace.harvest.oai.RecordTypeParser;
import cz.registrdigitalizace.harvest.oai.XmlContext;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.openarchives.oai2.HeaderType;
import org.openarchives.oai2.OAIPMHerrorType;
import org.openarchives.oai2.OAIPMHerrorcodeType;
import org.openarchives.oai2.OAIPMHtype;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


// 2018.03.15 - dočasně zablokováno propojování periodik v: HledejPeriodikumHledani


/**
 * Harvests digitized objects and stores them in Digitization Registry CZ.
 *
 * TODO:
 * XXX log exceptions to DB
 *
 * @author Jan Pokorsky
 */
public final class Harvest {
    private static final int pocetZpracovavanychDni = 7;

    public static final String CONFIG_PROPERTY = Harvest.class.getName() + ".config";

    private static final Logger LOG = Logger.getLogger(Harvest.class.getName());

    private final DigitizationRegistrySource dataSource;
    private final XmlContext xmlCtx = new XmlContext();
    private final OaiSourceFactory oaiFactory;
    private static Configuration conf;
    private final Map<String, ModsMetadataParser> parserMap = new HashMap<String, ModsMetadataParser>();
    
    private List<String> zpracovaneIdentifiers = new ArrayList<String>();
    private List<String> chybneIdentifiers = new ArrayList<String>();

    private static BufferedWriter bwSouborProNenalezeneZaznamyMonografie;
    private static BufferedWriter bwSouborProNalezeneZaznamyMonografie;
    private static BufferedWriter bwSouborProChybneZaznamyMonografie;
    private static BufferedWriter bwSouborProNejednoznacneZaznamyMonografie;
    private static BufferedWriter bwSouborProNenalezeneZaznamyPeriodika;
    private static BufferedWriter bwSouborProNalezeneZaznamyPeriodika;
    private static BufferedWriter bwSouborProChybneZaznamyPeriodika;
    private static BufferedWriter bwSouborProNejednoznacneZaznamyPeriodika;

    private static BufferedWriter bwSqlPrikazy;
    
    private static String sqlPrikazDocasnyText = "";
    private Boolean zapisDoDatabaze = true;
    
    private Connection connection = null;

    /**
     * 
     */
    public Harvest() {
        this(OaiSourceFactory.getInstance());
    }

    /**
     * 
     * @param oaiFactory (OaiSourceFactory)
     */
    Harvest(OaiSourceFactory oaiFactory) {
        this(oaiFactory, new Configuration());
    }

    /**
     * 
     * @param oaiFactory (OaiSourceFactory
     * @param conf (Configuration)
     */
    Harvest(OaiSourceFactory oaiFactory, Configuration conf) {
        this.oaiFactory = oaiFactory;
        this.dataSource = new DigitizationRegistrySource(conf.getProperties());
        this.conf = conf;
    }

    /**
     * @param args (String) the command line arguments
     */
    public static void main(String[] args) {
        try {
            conf = new Configuration();

            conf.loadConfigFile();
            conf.fillPropertiesFromConfigFile(); //zakladni konfigurace je v souboru, muze byt ale prepsana z prikazove radky
            conf.fromCmdLine(args, conf);

            if (!conf.getErrors().isEmpty()) {
                String errMsg = Utils.toString(conf.getErrors(), "\n");
                System.out.println(errMsg);
                System.out.println();
                System.out.println(conf.printHelp());
                throw new IllegalStateException(errMsg);
            } else if (conf.isVersion()) {
                String implementationVersion = Harvest.class.getPackage().getImplementationVersion();
                System.out.println("harvest: " + implementationVersion);
                return ;
            } else if (conf.isHelp()) {
                System.out.println(conf.printHelp());
                return ;
            }
            
            try {
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
                Date date = new Date();
                String datumSpusteni = dateFormat.format(date);
//                File f = new File("MonografieNenalezeno-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                File f = new File("./out/MonNen-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNenalezeneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNenalezeneZaznamyMonografie.write("uuid;pole001;carKod;signatura;ccnb;issn\n");
                } else {
                    bwSouborProNenalezeneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("MonografieNalezeno-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/MonNal-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNalezeneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNalezeneZaznamyMonografie.write("pole001;sigla;uuid\n");
                } else {
                    bwSouborProNalezeneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("MonografieChyba-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/MonChy-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProChybneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProChybneZaznamyMonografie.write("uuid;pole001;carKod;signatura;ccnb;issn\n");
                } else {
                    bwSouborProChybneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("MonografieNejednoznacne-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/MonoNej-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNejednoznacneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNejednoznacneZaznamyMonografie.write("uuid;pole001;carKod;signatura;ccnb;issn;idCisla\n");
                } else {
                    bwSouborProNejednoznacneZaznamyMonografie = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("PeriodikaNenalezeno-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/PerNen-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNenalezeneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNenalezeneZaznamyPeriodika.write("uuid;pole001;carKod;signatura;ccnb;issn\n");
                } else {
                    bwSouborProNenalezeneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("PeriodikaNalezeno-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/PerNal-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNalezeneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNalezeneZaznamyPeriodika.write("pole001;sigla;uuid\n");
                } else {
                    bwSouborProNalezeneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("PeriodikaChyba-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/PerChy-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProChybneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProChybneZaznamyPeriodika.write("uuid;pole001;carKod;signatura;ccnb;issn\n");
                } else {
                    bwSouborProChybneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
//                f = new File("PeriodikaNejednoznacne-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                f = new File("./out/PerNej-" + datumSpusteni + ".csv"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSouborProNejednoznacneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                    bwSouborProNejednoznacneZaznamyPeriodika.write("uuid;pole001;carKod;signatura;ccnb;issn;idCisla\n");
                } else {
                    bwSouborProNejednoznacneZaznamyPeriodika = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }

                f = new File("./out/SQLPrikazy-" + datumSpusteni + ".txt"); // kontrola na opakované spuštění v rámci dne
                if (!f.exists()) {
                    bwSqlPrikazy = new BufferedWriter(new FileWriter(f.getAbsolutePath()));
                } else {
                    bwSqlPrikazy = new BufferedWriter(new FileWriter(f.getAbsolutePath(), true));
                }
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, " chyba při otevírání pomocných souborů pro zápis " + ex.getMessage());
            }

            Harvest harvest = new Harvest(OaiSourceFactory.getInstance(), conf);
            LOG.log(Level.INFO, " harvest: " + conf.isHarvestData());
            if (conf.isHarvestData()) {
                LOG.log(Level.INFO, "  budu sklizet zaznamy");
                harvest.harvestData();
                LOG.log(Level.INFO, "  zaznamy sklizeny");
            }
            if (conf.isCreateThumbnails()) {
                // tato možnost zatím není zpracovaná a nebude. obrázky již nebudou potřeba. jen UUID digitálního záznamu, které se získává harvesterem.
                System.out.println(" !!!!Tato možnost není zpracovaná, protože již není potřeba!!!! "); // a možná ani nebude
            }
            LOG.log(Level.INFO, " spojit: " + conf.isSpojitPredlohuObjekt());
            if (conf.isSpojitPredlohuObjekt()) {
                LOG.log(Level.INFO, " začínám spojovat stávající záznamy předlohy a digObjektů");
                harvest.najitPredlohyBezDigObjektu();
                LOG.log(Level.INFO, " konec spojování");
            }
            try {
                LOG.log(Level.INFO, " uzavírám soubory");
                if (bwSouborProNenalezeneZaznamyMonografie != null) {
                    bwSouborProNenalezeneZaznamyMonografie.flush();
                    bwSouborProNenalezeneZaznamyMonografie.close();
                }
                if (bwSouborProNalezeneZaznamyMonografie != null) {
                    bwSouborProNalezeneZaznamyMonografie.flush();
                    bwSouborProNalezeneZaznamyMonografie.close();
                }
                if (bwSouborProChybneZaznamyMonografie != null) {
                    bwSouborProChybneZaznamyMonografie.flush();
                    bwSouborProChybneZaznamyMonografie.close();
                }
                if (bwSouborProNejednoznacneZaznamyMonografie != null) { 
                    bwSouborProNejednoznacneZaznamyMonografie.flush();
                    bwSouborProNejednoznacneZaznamyMonografie.close();
                }
                if (bwSouborProNenalezeneZaznamyPeriodika != null) { 
                    bwSouborProNenalezeneZaznamyPeriodika.flush();
                    bwSouborProNenalezeneZaznamyPeriodika.close();
                }
                if (bwSouborProNalezeneZaznamyPeriodika != null) { 
                    bwSouborProNalezeneZaznamyPeriodika.flush();
                    bwSouborProNalezeneZaznamyPeriodika.close();
                }
                if (bwSouborProChybneZaznamyPeriodika != null) { 
                    bwSouborProChybneZaznamyPeriodika.flush();
                    bwSouborProChybneZaznamyPeriodika.close();
                }
                if (bwSouborProNejednoznacneZaznamyPeriodika != null) { 
                    bwSouborProNejednoznacneZaznamyPeriodika.flush();
                    bwSouborProNejednoznacneZaznamyPeriodika.close();
                }
                if (bwSqlPrikazy != null) { 
                    bwSqlPrikazy.flush();
                    bwSqlPrikazy.close();
                }
                LOG.log(Level.INFO, " soubory uzavřeny v pořádku");
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, " chyba (main) při ukončování informačních souborů" + ex.getMessage());
            }
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, "Cannot start harvest process" + ex.getMessage());
            System.exit(1);
        }
    }

    /**
     * Harvests meta data descriptors from digital libraries.
     * @throws DaoException
     * @throws IOException 
     */
    public void harvestData() throws DaoException, IOException {
        //if (conf.isZapisDoDatabaze()) {
            this.zapisDoDatabaze = conf.isZapisDoDatabaze();
        //}
        Boolean pokracujVeZpracovani = true;
        
        // kontrola na správnost zadaných termínů - pokud zadány v jiném formátu, bude odmítnuto
        if ((!"".equals(conf.getFromDate())) || (!"".equals(conf.getToDate()))) {
            Date datumPomocny;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            try {
                if ((conf.getFromDate() != null) && (!"".equals(conf.getFromDate()))) {
                    datumPomocny = simpleDateFormat.parse(conf.getFromDate());
                }
                if ((conf.getToDate() != null) && (!"".equals(conf.getToDate()))) {
                    datumPomocny = simpleDateFormat.parse(conf.getToDate());
                }
            } catch (ParseException ex) {
                pokracujVeZpracovani = false;
                LOG.log(Level.SEVERE, " chyba při parsování termínu" + ex.getMessage());
            }
        }
        
        if (pokracujVeZpracovani) { //povedlo se parsovat termíny, takže lze pokračovat
            List<Library> libraries = fetchLibraries();
            for (Library library : libraries) {
                try {
                    LOG.log(Level.INFO, "  Začínám harvestovat knihovnu: " + library.getId());
                    harvestLibrary(library);
                } catch (Exception ex) {
                    // catch everything not to break other libraries processing
                    LOG.log(Level.SEVERE, "  Chyba (harvestData) zpracování knihovny (ID=" + library.getId() + ")", ex.getMessage());
                }
            }
        } else {
            LOG.log(Level.SEVERE, "  Chyba při zpracování zadaného datumu jako začátku nebo konce načítaných dat! Datum musí být zadán následujícím způsobem: '1999-12-31T00:00:00Z' (datum včetně času)." + Utils.lineNumber());
        }
    }

    /**
     * nastaví knihovny, které se mají zpracovat
     * @return (List<Library>)
     * @throws DaoException 
     */
    private List<Library> fetchLibraries() throws DaoException {
        LibraryDao libraryDao = new LibraryDao();
        LOG.log(Level.INFO, " načítám knihovny pro zpracování");
        HarvestTransaction transaction = new HarvestTransaction(dataSource);
        libraryDao.setDataSource(transaction);
        
        try {
            transaction.begin();
            //nastavení proměnných pro načtení knihovny, případně termínů od - do
            if (!"".equals(conf.getLibraryIds())) {
                libraryDao.setVybraneKnihovny(conf.getLibraryIds());
            }
            if (!"".equals(conf.getFromDate())) {
                libraryDao.setFromDate(conf.getFromDate());
            }
            if (!"".equals(conf.getToDate())) {
                libraryDao.setToDate(conf.getToDate());
            }
            List<Library> libraries = libraryDao.find();
            
            ////dočasně/docasne specifikováno přímo, bez načítání z Db.
            /*
            List<Library> libraries = new ArrayList<Library>();
            Library library = new Library();
            library.setId(new BigDecimal("463"));
            library.setHarvestProtocol("oaipmh");
            library.setMetadataFormat("drkramerius4");
            library.setLastHarvest("2016-11-09T00:00:00Z");
            library.setQueryParameters("");
            ////library.setBaseUrl("http://kramerius.lib.cas.cz/oaiprovider/");
            library.setBaseUrl("http://kramerius4.nkp.cz/oaiprovider/");
            ////library.setDListValue("ABA007-DK");
            library.setDListValue("ABA001-DK");
            ////library.setFromDate("2017-06-04T00:00:00Z");
            ////library.setToDate("2017-07-01T00:00:00Z");
            //library.setFromDate("2017-06-05T10:57:10Z");
            //library.setToDate("2017-06-06T00:00:00Z");
            library.setToDate("");

            libraries.add(library);
            */
            //konec lokálních dočasných změn
            return libraries;
        } finally {
            transaction.close();
        }
    }

    /**
     * Harvests library meta data and persists them in DB.
     * The persistence is optional.
     * @param library (Library)
     * @throws OaiException
     * @throws DaoException
     * @throws JAXBException
     * @throws XMLStreamException
     * @throws IOException 
     */
    private void harvestLibrary(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
        Library localModifiedLibrary;

        String datumOdLocalStr = library.getFromDate();
        String datumDoLocalStr = library.getToDate();
        Date datumOdLocal;
        Date datumDoLocal;
        Date datumZacatek;
        Date datumPomocny;
        Date datumDnesek = new Date();
        Date datumDo = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat simpleDateFormat3 = new SimpleDateFormat("yyyy.MM.dd");
        Calendar c = Calendar.getInstance();
        Boolean pokracuj = true;

        System.out.println(" zpracovavam knihovnu: " + library.getId().toString());
        try {
            datumDo = simpleDateFormat2.parse(datumDoLocalStr);
        } catch (ParseException ex) {
            System.out.println("Exception when set datumDo: " + ex.getMessage());
            LOG.log(Level.SEVERE, "Exception when set datumDo: " + ex.getMessage());
        }
        try {
            datumDnesek = simpleDateFormat2.parse(simpleDateFormat2.format(datumDnesek));
            c.setTime(simpleDateFormat.parse(datumOdLocalStr));
            datumOdLocal = c.getTime();
        } catch (ParseException ex) {
            try {
                c.setTime(simpleDateFormat2.parse(datumOdLocalStr));
                datumOdLocal = c.getTime();
            } catch (ParseException ex2) {
                LOG.log(Level.SEVERE, "Exception when set datumOdLocal: " + ex2.getMessage());
                pokracuj = false;
                datumOdLocal = null;
            }
        }
        if (datumDo == null) {
            try {
                datumDo = simpleDateFormat2.parse(simpleDateFormat2.format(datumDnesek));
            } catch (ParseException ex) {
                System.out.println("Exception when set datumDo: " + ex.getMessage());
                LOG.log(Level.SEVERE, "Exception when set datumDo: " + ex.getMessage());
                pokracuj = false;
            }
        }

        LOG.log(Level.INFO, " datumOd: " + datumOdLocal.toString() + " -- datumDo: " + datumDo.toString());
        if ((pokracuj) && (datumOdLocal != null)) {
            LOG.log(Level.INFO, " jsem v knihovne");
            //System.out.println(" jsem v knihovne");
            localModifiedLibrary = library;
            datumZacatek = datumOdLocal;
            pokracuj = true;
            int pocetZpracovanychDni = 0;
            while ((datumOdLocal.before(datumDo)) && (pokracuj) && (pocetZpracovanychDni<this.pocetZpracovavanychDni)) { //počet dní, které se mají harvestovat
                pocetZpracovanychDni++;
                if (datumZacatek.equals(datumOdLocal)) {
                    try {
                        datumPomocny = simpleDateFormat.parse(simpleDateFormat2.format(datumOdLocal) + "T00:00:00Z");
                    } catch (ParseException ex) {
                        pokracuj = false;
                        datumPomocny = null;
                        LOG.log(Level.SEVERE, " nepodařilo se vytvořit pomocné datum" + ex.getMessage());
                    }
                } else {
                    datumPomocny = datumOdLocal;
                }
                LOG.log(Level.INFO, " pokracuj: " + pokracuj);
                if ((pokracuj) && (datumPomocny != null)) {
                    LOG.log(Level.INFO, " zpracovavam datum: " + simpleDateFormat3.format(datumPomocny));
                    c.setTime(datumPomocny);
                    c.add(Calendar.DAY_OF_MONTH, 1);
                    datumDoLocal =  c.getTime();
                    localModifiedLibrary.setToDate(simpleDateFormat.format(datumDoLocal));

                    Boolean nacteno = false;
                    int pocitadloMonografie = 0;
                    int pocitadloMonografieUnit = 0;
                    int pocitadloPeriodika = 0;
                    int pocitadloPeriodikaItem = 0;
                    Boolean nactenoMonografie = false;
                    Boolean nactenoMonografieUnit = false;
                    Boolean nactenoPeriodika = false;
                    Boolean nactenoPeriodikaItem = false;
                    while ((!nactenoMonografie) && (pocitadloMonografie < 5)) {
                        try {
                            harvestLibraryOneDay(localModifiedLibrary, "&set=monograph");
                            nactenoMonografie = true;
                        } catch (cz.registrdigitalizace.harvest.oai.OaiException ex) {
                            pocitadloMonografie++;
                            LOG.log(Level.INFO, ("  pokus číslo: " + pocitadloMonografie + " " + ex.getMessage()));
                        }
                    }
                    /*
                    while ((!nactenoMonografieUnit) && (pocitadloMonografieUnit < 5)) {
                        try {
                            harvestLibraryOneDay(localModifiedLibrary, "&set=monographunit"); //asi neexistuje
                            nactenoMonografieUnit = true;
                        } catch (cz.registrdigitalizace.harvest.oai.OaiException ex) {
                            pocitadloMonografieUnit++;
                            LOG.log(Level.INFO, ("  pokus číslo: " + pocitadloMonografieUnit + " " + ex.getMessage()));
                        }
                    }
                    */
                    while ((!nactenoPeriodika) && (pocitadloPeriodika < 5)) {
                        try {
                            harvestLibraryOneDay(localModifiedLibrary, "&set=periodical");
                            nactenoPeriodika = true;
                        } catch (cz.registrdigitalizace.harvest.oai.OaiException ex) {
                            pocitadloPeriodika++;
                            LOG.log(Level.INFO, ("  pokus číslo: " + pocitadloPeriodika + " " + ex.getMessage()));
                        }
                    }
                    while ((!nactenoPeriodikaItem) && (pocitadloPeriodikaItem < 5)) {
                        try {
                            harvestLibraryOneDay(localModifiedLibrary, "&set=periodicalitem"); // tyto záznamy se nespojují
                            nactenoPeriodikaItem = true;
                        } catch (cz.registrdigitalizace.harvest.oai.OaiException ex) {
                            pocitadloPeriodikaItem++;
                            LOG.log(Level.INFO, ("  pokus číslo: " + pocitadloPeriodikaItem + " " + ex.getMessage()));
                        }
                    }
                    if ((nactenoMonografie) || (nactenoMonografieUnit) || (nactenoPeriodika) || (nactenoPeriodikaItem)) {
                        nacteno = true;
                    }

                    if (!nacteno) {
                        pokracuj = false;
                    }
                    datumOdLocal =  datumDoLocal;
                    localModifiedLibrary.setFromDate(simpleDateFormat.format(datumDoLocal));
                    Connection connectionLocal = null;
                    PreparedStatement statement = null;
                    try {
                        connectionLocal = this.dataSource.getConnection();
                        statement = connectionLocal.prepareStatement("update digknihovna set lastharvest='" + simpleDateFormat.format(datumDoLocal) + "' where id=" + localModifiedLibrary.getId());
                        LOG.log(Level.INFO, " sql pro update poslední sklizně: " + ((oracle.jdbc.driver.OraclePreparedStatement) statement).getOriginalSql());
                        statement.execute();
                        connectionLocal.commit();
                    } catch (SQLException ex) {
                        LOG.log(Level.SEVERE, " chyba při update datumu posledního harvesteru " + ex.getMessage());
                    } finally {
                        Utils.tryClose(statement);
                        Utils.tryClose(connectionLocal);
                    }
                }

            }
            if (!pokracuj) {
                //System.out.println("  zpracovani preruseno pro timeout");
                LOG.log(Level.INFO, "  zpracování přerušeno pro timeout");
            }
            //System.out.println("  jsem za nactenim zaznamu pro jednotlive datumy");
            
        }

    }

    
    /**
     * načítá záznamy z knihovny pro jednotlivé dny a ty pak zpracovává
     * @param library (Library)
     * @throws OaiException
     * @throws DaoException
     * @throws JAXBException
     * @throws XMLStreamException
     * @throws IOException 
     */
    private void harvestLibraryOneDay(Library library, String queryParameter) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
        Boolean pokracuj = true;
        //System.out.println(" oneDayHarvest start");
        LOG.log(Level.INFO, " oneDayHarvest start - zpracovávám: " + queryParameter);
        long time = System.currentTimeMillis();
        Library localLibrary = library;
        localLibrary.setVerbParameter("ListIdentifiers");
        //localLibrary.setVerbParameter("ListRecords");
        //localLibrary.setQueryParameters("&set=monograph");
        localLibrary.setQueryParameters(queryParameter);
        
        List<String> seznamIdentifiers = new ArrayList<String>();
        try {
            seznamIdentifiers = GetIdentifiers(library, null);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, " chyba (harvestLibraryOneDay): " + ex.getMessage());
            pokracuj = false;
        }

        //LOG.log(Level.INFO, " počet identifikátorů: " + seznamIdentifiers.size());
        if ((pokracuj) && (seznamIdentifiers.size()>0)) {
            try {
                this.connection = this.dataSource.getConnection();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, " nepodařilo se připojit na databázi", ex.getMessage());
            }

            if (seznamIdentifiers.size()>0) {
                for (int i = 0; i < seznamIdentifiers.size(); i++) {
                    LOG.log(Level.INFO, " identifiers: " + seznamIdentifiers.get(i));
                    //System.out.println(" identifiers: " + seznamIdentifiers.get(i));
                    KrameriusEntry krameriusEntry = new KrameriusEntry();
                    krameriusEntry = getEntry(library, seznamIdentifiers.get(i));
                    Boolean zaznamZalozen = false;
                    zaznamZalozen = zalozZaznam(library, krameriusEntry);
                    LOG.log(Level.INFO, " zaznam zalozen: " + zaznamZalozen);
                    //System.out.println(" zaznam zalozen: " + zaznamZalozen);
                    Boolean pripojPredlohu = false;
//                    pripojPredlohu = pripojPredlohu(library, krameriusEntry);
                    pripojPredlohu = pripojPredlohu(krameriusEntry);
                    LOG.log(Level.INFO, " predloha pripojena: " + pripojPredlohu);
                    //System.out.println(" predloha pripojena: " + pripojPredlohu);
                    LOG.log(Level.INFO, " \n\n dalsiZaznam\n\n");
                    //System.out.println(" \n\ndalsiZaznam\n\n");
                    krameriusEntry = null;
                }
            }
            Utils.tryClose(this.connection);
        } else {
            LOG.log(Level.INFO, " den vynechán");
        }
    }

    /**
     * 
     * @param library
     * @param resumptionToken
     * @throws OaiException
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws DOMException 
     */
//    private List<String> GetIdentifiers(Library library, String resumptionTokenStr) throws OaiException, IOException, ParserConfigurationException, SAXException, DOMException {
    private List<String> GetIdentifiers(Library library, String resumptionTokenStr) throws OaiException, IOException, ParserConfigurationException, DOMException {
        System.out.println(" resumptionToken input: " + resumptionTokenStr);
        String resumptionToken = "";
        int cursorLocal = 0;
        Boolean pokracuj = false;
        List<String> seznamIdentifiers = new ArrayList<String>();
        //OaiSource oaiSource = resolveOaiSource(library);
        OaiSource oaiSource = null;
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                oaiSource = oaiFactory.createListRecords(
                        library.getBaseUrl(), library.getVerbParameter(), library.getLastHarvest(), library.getFromDate(), library.getToDate(),
                        library.getMetadataFormat(), library.getQueryParameters(), resumptionTokenStr);
            } catch (OaiException ex) {
                //System.out.println(" chyba OAI: " + ex.getMessage());
                LOG.log(Level.SEVERE, " chyba OAI" + ex.getMessage());
            }
        } else {
            //System.out.println("chybny protokol");
            LOG.log(Level.INFO, " chybný protokol ");
        }
                
        if (oaiSource == null) {
            LOG.log(Level.INFO, "Skip {0}", library);
            return null;
        }
        LOG.log(Level.INFO, "Harvesting {0}", library);

        InputStream streamLocal = oaiSource.openConnection();
        LOG.log(Level.INFO, " otevírám spojení");
        DocumentBuilderFactory dbFactoryLocal = DocumentBuilderFactory.newInstance();
        LOG.log(Level.INFO, " builderFactory");
        DocumentBuilder dBuilderLocal = dbFactoryLocal.newDocumentBuilder();
        LOG.log(Level.INFO, " documentBuilder");
        Document docLocal = null;
        Boolean neniChyba = true;
        try {
            docLocal = dBuilderLocal.parse(streamLocal);
        } catch (SAXParseException ex) { //potřebuji hlavně: SAXParseException
            LOG.log(Level.INFO, " chyba při vytváření dokumentu - pravděpodobně je bez dat: " + ex + " " + ex.getMessage());
            neniChyba = false;
        } catch (SAXException ex) { //potřebuji hlavně: SAXParseException
        }

        if (neniChyba) {
            NodeList nListIdentifiers = docLocal.getElementsByTagName("ListIdentifiers");
            Node nNodeIdentifiers = nListIdentifiers.item(0);
            NodeList nListChild = nNodeIdentifiers.getChildNodes();
            for (int i = 0; i < nListChild.getLength(); i++) {
                Node nNodeChild = nListChild.item(i);
                if ("header".equals(nNodeChild.getNodeName())) {
                    NodeList nListHeader = nNodeChild.getChildNodes();
                    for (int j = 0; j < nListHeader.getLength(); j++) { //zpracovani zaznamu 
                        Node nNodeHeader = nListHeader.item(j);
                        if ("identifier".equals(nNodeHeader.getNodeName())) {
                            seznamIdentifiers.add(nNodeHeader.getTextContent());
                        }
                    }
                }
                
                if ("resumptionToken".equals(nNodeChild.getNodeName())) {
                    NodeList nListToken = nNodeChild.getChildNodes();
                    Node nNodeToken = nListToken.item(0);
                    resumptionToken = nNodeToken.getTextContent();
                    pokracuj = true;
                }
            }
            
            if ((pokracuj) && (resumptionToken.equals(resumptionTokenStr))) {
                //System.out.println(" vracen stejny token - nelze pokracovat");
                LOG.log(Level.INFO, " vrácen stejný token - nelze pokračovat");
                pokracuj = false;
            }

            if (pokracuj) {
                List<String> seznamIdentifiersLocal = new ArrayList<String>();
                seznamIdentifiersLocal = GetIdentifiers(library, resumptionToken);
                if (seznamIdentifiersLocal.size()>0) {
                    seznamIdentifiers.addAll(seznamIdentifiersLocal);
                }
            }
        } else {
            LOG.log(Level.INFO, " toto načítání vynecháno pro chybu");
        }

        return seznamIdentifiers;
    }

    private KrameriusEntry getEntry(Library library, String identifier) {
        KrameriusEntry krameriusEntry = new KrameriusEntry();

        Boolean opakuj = true;
        NodeList metadata = null;
        int pocetOpakovani = 0;
        while ((opakuj) && (pocetOpakovani<5) ) {
            try {
                System.out.println("zkousim nacist data - pokus c: " + pocetOpakovani);
                metadata = getEntryValue(library, identifier);
                if (metadata != null) {
                    opakuj = false;
                }
            } catch (OaiException ex) {
                LOG.log(Level.SEVERE, " chyba oai: " + ex.getMessage());
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, " chyba io: " + ex.getMessage());
            } catch (ParserConfigurationException ex) {
                this.chybneIdentifiers.add(identifier);
                LOG.log(Level.SEVERE, " chyba parsovani: " + ex.getMessage());
                //opakuj = false;
            } catch (SAXException ex) {
                this.chybneIdentifiers.add(identifier);
                LOG.log(Level.SEVERE, " chyba sax: " + ex.getMessage());
                //opakuj = false;
            }
            pocetOpakovani++;
        }
        if ((metadata != null) && (metadata.getLength()>0)) {
            System.out.println(" metadata nactena");
            this.zpracovaneIdentifiers.add(identifier);
        
            krameriusEntry.setLibraryId(library.getId().toString());
            krameriusEntry.setDListValue(library.getDListValue());
            Node nNodeRecord = metadata.item(0);
            String uuidStr = null;
            String typeStr = null;
            if ("dr:record".equals(nNodeRecord.getNodeName())) {
                NodeList nListRecordChild = nNodeRecord.getChildNodes();
                for (int i=0; i<nListRecordChild.getLength(); i++) {
                    Node nNodeRecordChild = nListRecordChild.item(i);
                    if ("dr:uuid".equals(nNodeRecordChild.getNodeName())) {
                        uuidStr = nNodeRecordChild.getTextContent();
                    }
                    if ("dr:type".equals(nNodeRecordChild.getNodeName())) {
                        typeStr = nNodeRecordChild.getTextContent();
                    }
                    if ("dr:descriptor".equals(nNodeRecordChild.getNodeName())) {
                        NodeList nListDescriptorChild = nNodeRecordChild.getChildNodes();
                        for (int j=0; j<nListDescriptorChild.getLength(); j++) {
                            Node nNodeDescriptorChild = nListDescriptorChild.item(j);
                            krameriusEntry.setUuid(uuidStr);
                            krameriusEntry.setDruhDokumentu(typeStr);
                            krameriusEntry = ZpracujXML(nNodeDescriptorChild, krameriusEntry);
                        }
                    }
                }
            }
            
            System.out.println(" metadata zpracovana");
        }
        
        return krameriusEntry;
    }

    private KrameriusEntry ZpracujXML(Node nNodeDescriptorChild, KrameriusEntry krameriusEntry) {
        //KrameriusEntry krameriusEntry = new KrameriusEntry();

        if ("mods:modsCollection".equals(nNodeDescriptorChild.getNodeName())) {
            NodeList nListMMCChild = nNodeDescriptorChild.getChildNodes();
            int zaznamCislo = 0;
            Boolean zaznamNalezen = false;
            for (int k=0; k<nListMMCChild.getLength(); k++) {
                Node nNodeMMCChild = nListMMCChild.item(k);
                if ((!zaznamNalezen) && ("mods:mods".equals(nNodeMMCChild.getNodeName()))) {
                    zaznamCislo = k;
                    zaznamNalezen = true;
                }
            }
            if (zaznamNalezen) {
                Node nNodeMMCChild = nListMMCChild.item(zaznamCislo); //podle xsl specifikace se zpracuje pouze 1. zaznam mods:mods
                if ("mods:mods".equals(nNodeMMCChild.getNodeName())) {
                    NodeList nListMMChild = nNodeMMCChild.getChildNodes();
                    for (int k=0; k<nListMMChild.getLength(); k++) {
                        Node nNodeMMChild = nListMMChild.item(k);
                        if ("mods:titleInfo".equals(nNodeMMChild.getNodeName())) {
                            NodeList nListChild = nNodeMMChild.getChildNodes();
                            String titleLocal = "";
                            String subTitleLocal = "";
                            String partNumberLocal = "";
                            String partNameLocal = "";
                            String displayLabelLocal = "";
                            Boolean alternativeLocal = false;
                            String typeLocal = "";
                            List<String> nezpracovaneHodnotyLocal = new ArrayList<String>();
                            for (int l=0; l<nListChild.getLength(); l++) {
                                Node nNodeChild = nListChild.item(l);
                                if ("mods:title".equals(nNodeChild.getNodeName())) {
                                    if (titleLocal.isEmpty()) {
                                        titleLocal = Utils.vratString(nNodeChild);
                                        typeLocal = "title";

                                        NamedNodeMap nListAttribute = nNodeMMChild.getAttributes();
                                        for (int m=0; m<nListAttribute.getLength(); m++) {
                                            Node nNodeAttribute = nListAttribute.item(m);
                                            if ("displayLabel".equals(nNodeAttribute.getNodeName())) {
                                                displayLabelLocal = Utils.vratString(nNodeAttribute);
                                            } else if ("type".equals(nNodeAttribute.getNodeName())) {
                                                if ("alternative".equals(Utils.vratString(nNodeAttribute))) {
                                                    alternativeLocal = true;
                                                }
                                            } else {
                                                nezpracovaneHodnotyLocal.add(Utils.vratString(nNodeAttribute));
                                            }
                                        }
                                    }
                                }
                                if ("mods:subTitle".equals(nNodeChild.getNodeName())) {
                                    subTitleLocal = Utils.vratString(nNodeChild);
                                }
                                if ("mods:partNumber".equals(nNodeChild.getNodeName())) {
                                    partNumberLocal = Utils.vratString(nNodeChild);
                                }
                                if ("mods:partName".equals(nNodeChild.getNodeName())) {
                                    partNameLocal = Utils.vratString(nNodeChild);
                                }
                            }
                            NazevEntry novyNazev = new NazevEntry();
                            novyNazev.setTitle(titleLocal);
                            novyNazev.setType(typeLocal);
                            novyNazev.setDisplayLabel(displayLabelLocal);
                            novyNazev.setAlternative(alternativeLocal);
                            novyNazev.addNezpracovaneHodnotyAll(nezpracovaneHodnotyLocal);
                            novyNazev.setSubTitle(subTitleLocal);
                            novyNazev.setPartNumber(partNumberLocal);
                            novyNazev.setPartName(partNameLocal);
                            krameriusEntry.addNazev(novyNazev);

                        }
                        if ("mods:originInfo".equals(nNodeMMChild.getNodeName())) {
                            String mistoLocal = "";
                            String vydavatelLocal = "";
                            String poradiVydaniLocal = "";
                            String ediceLocal = "";
                            String vydaniLocal = "";
                            Boolean datumDoVydani = false;
                            String datumDoVydaniStr = "";
                            List<String> datumVydaniLocal = new ArrayList<String>();
                            NodeList nlChild = nNodeMMChild.getChildNodes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("mods:place".equals(nChild.getNodeName())) {
                                    NodeList nlCChild = nChild.getChildNodes();
                                    for (int m=0; m<nlCChild.getLength(); m++) {
                                        Node nCChild = nlCChild.item(m);
                                        if ("mods:placeTerm".equals(nCChild.getNodeName())) {
                                            NamedNodeMap nlCCAttribute = nCChild.getAttributes();
                                            for (int n=0; n<nlCCAttribute.getLength(); n++) {
                                                Node nCCAttribute = nlCCAttribute.item(n);
                                                if ("type".equals(nCCAttribute.getNodeName())) {
                                                    if ("text".equals(Utils.vratString(nCCAttribute))) {
                                                        mistoLocal = Utils.vratString(nCChild);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if ("mods:publisher".equals(nChild.getNodeName())) {
                                    vydavatelLocal = Utils.vratString(nChild);
                                }
                                if ("mods:edition".equals(nChild.getNodeName())) {
                                    poradiVydaniLocal = Utils.vratString(nChild);
                                }
                                if ("mods:series".equals(nChild.getNodeName())) { //nemam overeno
                                    ediceLocal = Utils.vratString(nChild);
                                }
                                if ("mods:issuance".equals(nChild.getNodeName())) {
                                    vydaniLocal = Utils.vratString(nChild);
                                    if (!"continuing".equals(Utils.vratString(nChild))) datumDoVydani = true;
                                }
                                if ("mods:dateIssued".equals(nChild.getNodeName())) {
                                    NamedNodeMap nlCAttribute = nChild.getAttributes();
                                    if (nlCAttribute.getLength()==0) {
                                        datumVydaniLocal.add(Utils.vratString(nChild));
                                    }
                                    if (datumDoVydani) {
                                        datumDoVydaniStr = " " + Utils.vratString(nChild);
                                    }
                                }
                            }
                            NakladatelEntry novyNakladatel = new NakladatelEntry();
                            novyNakladatel.setPoradiVydani(poradiVydaniLocal);
                            novyNakladatel.setEdice(ediceLocal);
                            novyNakladatel.setMistoVydani(mistoLocal);
                            novyNakladatel.setNakladatel(vydavatelLocal);
                            novyNakladatel.setDatumVydani(datumDoVydaniStr);
                            novyNakladatel.setVydani(vydaniLocal + datumDoVydaniStr);
                            krameriusEntry.addNakladatel(novyNakladatel);
                            krameriusEntry.setDatumVydani(datumVydaniLocal);
                        }
                        if ("mods:name".equals(nNodeMMChild.getNodeName())) {
                            List<String> autorLocal = new ArrayList<String>();
                            String roleLocal = "";
                            String rokLocal = "";
                            String familyLocal = "";
                            String displayFormLocal = "";
                            List<String> givenLocal = new ArrayList<String>();
                            NodeList nlChild = nNodeMMChild.getChildNodes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("mods:namePart".equals(nChild.getNodeName())) {
                                    NamedNodeMap nlCAttribute = nChild.getAttributes();
                                    if (nlCAttribute.getLength()>0) {
                                        for (int n=0; n<nlCAttribute.getLength(); n++) {
                                            Node nCAttribute = nlCAttribute.item(n);
                                            if ("type".equals(nCAttribute.getNodeName())) {
                                                if ("date".equals(Utils.vratString(nCAttribute))) {
                                                    rokLocal = Utils.vratString(nChild);
                                                }
                                            } else if ("family".equals(nCAttribute.getNodeName())) {
                                                familyLocal = Utils.vratString(nChild);
                                            } else if ("given".equals(nCAttribute.getNodeName())) {
                                                givenLocal.add(Utils.vratString(nChild));
                                            }
                                        }
                                    } else {
                                        autorLocal.add(Utils.vratString(nChild));
                                    }
                                } else if ("mods:role".equals(nChild.getNodeName())) {
                                    NamedNodeMap nlCAttribute = nChild.getAttributes();
                                    if (nlCAttribute.getLength()>0) {
                                        String authorityLocal = "";
                                        String typeLocal = "";
                                        for (int n=0; n<nlCAttribute.getLength(); n++) {
                                            Node nCAttribute = nlCAttribute.item(n);
                                            if ("type".equals(nCAttribute.getNodeName())) {
                                                typeLocal = Utils.vratString(nCAttribute);
                                            //} else if ("authority".equals(nNodeAttribute.getNodeName())) {
                                            //    authorityLocal = Utils.vratString(nNodeAttribute);
                                            }
                                        }
                                        //if (("code".equals(typeLocal)) && ("marcrelator".equals(authorityLocal))) {
                                        if ("code".equals(typeLocal)) {
                                            roleLocal = Utils.vratString(nChild);
                                        }
                                    }
                                } else if ("mods:displayForm".equals(nChild.getNodeName())) {
                                    displayFormLocal = " (" + Utils.vratString(nChild) + ") ";
                                }
                            }
                            if (("cre".equals(roleLocal)) || ("aut".equals(roleLocal))) {
                                AutorEntry novyAutor = new AutorEntry();
                                novyAutor.setAutor(autorLocal);
                                novyAutor.setRozmezi(rokLocal);
                                novyAutor.setRole(roleLocal);
                                krameriusEntry.addAutor(novyAutor);
                            }
                        }

                        //if ("mods:typeOfResource".equals(nNodeMMChild.getNodeName())) {
                        //}
                        if ("mods:genre".equals(nNodeMMChild.getNodeName())) {
                            krameriusEntry.setGenre(Utils.vratString(nNodeMMChild));
                        }
                        //if ("mods:language".equals(nNodeMMChild.getNodeName())) {
                        //}
                        //if ("mods:physicalDescription".equals(nNodeMMChild.getNodeName())) {
                        //}
                        //if ("mods:targetAudience".equals(nNodeMMChild.getNodeName())) {
                        //}
                        //if ("mods:note".equals(nNodeMMChild.getNodeName())) {
                        //}
                        if ("mods:location".equals(nNodeMMChild.getNodeName())) {
                            NodeList nlChild = nNodeMMChild.getChildNodes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("mods:physicalLocation".equals(nChild.getNodeName())) {
                                    krameriusEntry.setSigla(Utils.vratString(nChild));
                                }
                                if ("mods:shelfLocator".equals(nChild.getNodeName())) {
                                    krameriusEntry.setSignatura(Utils.vratString(nChild));
                                }
                            }
                        }
                        //if ("mods:relatedItem".equals(nNodeMMChild.getNodeName())) {
                        //}
                        if ("mods:identifier".equals(nNodeMMChild.getNodeName())) {
                            String fieldTypeStr = "";
                            Boolean fieldInvalid = false;
                            NamedNodeMap nlChild = nNodeMMChild.getAttributes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("type".equals(nChild.getNodeName())) {
                                    fieldTypeStr = Utils.vratString(nChild);
                                }
                                if ("invalid".equals(nChild.getNodeName())) {
                                    fieldInvalid = true;
                                }
                            }
                            if ("barCode".equals(fieldTypeStr)) {
                                krameriusEntry.setCarKod(Utils.vratString(nNodeMMChild));
                            } else if ("issn".equals(fieldTypeStr)) {
                                if (fieldInvalid) {
                                    krameriusEntry.addNepIssn(Utils.vratString(nNodeMMChild));
                                } else {
                                    krameriusEntry.addIssn(Utils.vratString(nNodeMMChild));
                                }
                            } else if ("isbn".equals(fieldTypeStr)) {
                                if (fieldInvalid) {
                                    krameriusEntry.addNepIsbn(Utils.vratString(nNodeMMChild));
                                } else {
                                    krameriusEntry.addIsbn(Utils.vratString(nNodeMMChild));
                                }
                            } else if ("urnnbn".equals(fieldTypeStr)) {
                                krameriusEntry.addUrnnbn(Utils.vratString(nNodeMMChild));
                            } else if ("ccnb".equals(fieldTypeStr)) {
                                if (fieldInvalid) {
                                    krameriusEntry.addNepCcnb(Utils.vratString(nNodeMMChild));
                                } else {
                                    krameriusEntry.addCcnb(Utils.vratString(nNodeMMChild));
                                }
                            } else if ("oclc".equals(fieldTypeStr)) {
                                krameriusEntry.addOclc(Utils.vratString(nNodeMMChild));
                            }
                        }
                        if ("mods:recordInfo".equals(nNodeMMChild.getNodeName())) {
                            NodeList nlChild = nNodeMMChild.getChildNodes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("mods:recordIdentifier".equals(nChild.getNodeName())) {
                                    krameriusEntry.setPole001(Utils.vratString(nChild));
                                    NamedNodeMap nlChildAttribute = nNodeMMChild.getAttributes();
                                    for (int m=0; m<nlChildAttribute.getLength(); m++) {
                                        Node nChildAttribute = nlChild.item(m);
                                        if ("source".equals(nChildAttribute.getNodeName())) krameriusEntry.setKatalog(Utils.vratString(nChildAttribute));
                                    }
                                }
                                if ("mods:recordContentSource".equals(nChild.getNodeName())) krameriusEntry.setSiglaBibUdaju(Utils.vratString(nChild));
                            }
                        }
                        //if ("mods:classification".equals(nNodeMMChild.getNodeName())) {
                        //}
                        if ("mods:part".equals(nNodeMMChild.getNodeName())) {
                            String volumeTitleLocal = "";
                            Boolean ziskejUnitNumber = false;
                            Boolean jeVolume = false;
                            String rokLocal = "";
                            String unitTitleLocal = "";
                            String issueTitleLocal = "";
                            Boolean jePeriodikum = false;
                            String cisloPeriodikaLocal = "";
                            NamedNodeMap nNodeAttribute = nNodeMMChild.getAttributes();
                            for (int l=0; l<nNodeAttribute.getLength(); l++) {
                                Node nlAttribute = nNodeAttribute.item(l);
                                if ("type".equals(nlAttribute.getNodeName())) {
                                    if ("volume".equals(Utils.vratString(nlAttribute))) ziskejUnitNumber = true;
                                    if ("PeriodicalIssue".equals(Utils.vratString(nlAttribute))) jePeriodikum = true;
                                }
                            }

                            NodeList nlChild = nNodeMMChild.getChildNodes();
                            for (int l=0; l<nlChild.getLength(); l++) {
                                Node nChild = nlChild.item(l);
                                if ("mods:detail".equals(nChild.getNodeName())) {
                                    Boolean jeIssue = false;
                                    NamedNodeMap nlCAttributes = nChild.getAttributes();
                                    for (int m=0; m<nlCAttributes.getLength(); m++) {
                                        Node nCAtrributes = nlCAttributes.item(m);
                                        if ("type".equals(nCAtrributes.getNodeName())) {
                                            if ("volume".equals(Utils.vratString(nCAtrributes))) {
                                                jeVolume = true;
                                            }
                                            if ("issue".equals(Utils.vratString(nCAtrributes))) {
                                                jeIssue = true;
                                            }
                                        }
                                    }

                                    if ((ziskejUnitNumber) || (jeIssue)) {
                                        NodeList nlCChild = nChild.getChildNodes();
                                        for (int m=0; m<nlCChild.getLength(); m++) {
                                            Node nCChild = nlCChild.item(m);
                                            if ("mods:number".equals(nCChild)) {
                                                if (jeIssue) issueTitleLocal = Utils.vratString(nCChild);
                                                if (jeVolume) volumeTitleLocal = Utils.vratString(nCChild);
                                                if ((jePeriodikum) && (jeIssue)) cisloPeriodikaLocal = Utils.vratString(nCChild);
                                            }
                                            unitTitleLocal = Utils.vratString(nlCChild.item(m));
                                        }
                                    }
                                }
                                if ("mods:date".equals(nChild.getNodeName())) {
                                    if (jeVolume) rokLocal = Utils.vratString(nChild);
                                }
                            }
                            if (!"".equals(rokLocal)) krameriusEntry.addRokVydani(rokLocal);
                            //if (!"".equals(issueTitleLocal)) krameriusEntry.setIssueTitle("Číslo: " + issueTitleLocal);
                            //if (!"".equals(volumeTitleLocal)) krameriusEntry.setVolumeTitle("Ročník: " + volumeTitleLocal);
                            //if (!"".equals(unitTitleLocal)) krameriusEntry.setUnitTitle("Část: " + unitTitleLocal);
                            if (!"".equals(issueTitleLocal)) krameriusEntry.setIssueTitle(issueTitleLocal);
                            if (!"".equals(volumeTitleLocal)) krameriusEntry.setVolumeTitle(volumeTitleLocal);
                            if (!"".equals(unitTitleLocal)) krameriusEntry.setUnitTitle(unitTitleLocal);
                            if (!"".equals(cisloPeriodikaLocal)) krameriusEntry.setCisloPeriodika(cisloPeriodikaLocal);
                        }
                    }
                }
            }
        }

        return krameriusEntry;
    }
    
    /**
     * načte záznam z OAI a vrátí jeho XML prezentaci jako NodeList
     * @param library
     * @param identifier
     * @return
     * @throws OaiException
     * @throws IOException 
     */
    private NodeList getEntryValue(Library library, String identifier) throws OaiException, IOException, ParserConfigurationException, SAXException {
        NodeList metadataVystup = null;
        OaiSource oaiSource = null;
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                oaiSource = oaiFactory.createListRecords(
                        library.getBaseUrl(), "GetRecord", null, null, null,
                        library.getMetadataFormat(), "&identifier=" + identifier, null);
            } catch (OaiException ex) {
                LOG.log(Level.SEVERE, " chyba oai: " + ex.getMessage());
            }
        } else {
            System.out.println("chybny protokol");
            LOG.log(Level.SEVERE, " chybný protokol: ");
        }
                
        if (oaiSource == null) {
            LOG.log(Level.FINE, "Skip {0}", identifier);
            return null;
        }
        LOG.log(Level.INFO, "Harvesting {0}", identifier);
        
        InputStream streamLocal = oaiSource.openConnection();
        DocumentBuilderFactory dbFactoryLocal = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilderLocal = dbFactoryLocal.newDocumentBuilder();
        Document docLocal = dBuilderLocal.parse(streamLocal);

        NodeList nListGetRecord = docLocal.getElementsByTagName("GetRecord");

        Node nNodeGetRecord = nListGetRecord.item(0);
        NodeList nListGetRecordChild = nNodeGetRecord.getChildNodes();
        Node nNodeGetRecordChild = nListGetRecordChild.item(0);
        if ("record".equals(nNodeGetRecordChild.getNodeName())) {
            NodeList nListRecord = nNodeGetRecordChild.getChildNodes();
            for (int i=0; i<nListRecord.getLength(); i++) {
                Node nNodeRecordr = nListRecord.item(i);
                if ("metadata".equals(nNodeRecordr.getNodeName())) {
                    metadataVystup = nNodeRecordr.getChildNodes();
                }
            }
        }
        return metadataVystup;
    }
    
    /**
     * načítá záznamy z knihovny pro jednotlivé dny a ty pak zpracovává
     * @param library (Library)
     * @throws OaiException
     * @throws DaoException
     * @throws JAXBException
     * @throws XMLStreamException
     * @throws IOException 
     */
    private void harvestLibraryOneDay2(Library library) throws OaiException, DaoException, JAXBException, XMLStreamException, IOException {
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
            // zpracovavam zaznam, pokud se vrati hodnota, znamena to, ze dalsi zaznam je porusen
            casPosledniZpracovanyZaznam = persistRecords(library, oaiRecords, time);
            if (!"".equals(casPosledniZpracovanyZaznam)) {
                //pokusim se posunout cas o 1 vterinu a pokracovat ve sklizeni
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
                        c.add(Calendar.SECOND, 1); //pridani vyse zminene vteriny
                        datumFromLocal =  c.getTime();
                        libraryLocal.setFromDate(simpleDateFormat.format(datumFromLocal));
                        // vzdy se musi nastavit novy objekt zdroje, jinak nepokracuje a skonci
                        OaiSource oaiSourceLocal = resolveOaiSource(library);
                        Harvester harvesterLocal = new Harvester(oaiSourceLocal, xmlCtx);
                        ListResult<Record> oaiRecordsLocal = harvester.getListRecords(true);
                        casPosledniZpracovanyZaznam = persistRecords(libraryLocal, harvesterLocal.getListRecords(true), System.currentTimeMillis());
                        oaiRecordsLocal.close();
                    } catch (ParseException ex) {
                        //pokusim se zapsat info o zaznamech, ktere nebyly zpracovany
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
                            System.out.println("  nepodarilo se zapsat info o nezpracovanem zaznamu" + Utils.lineNumber());
                            LOG.log(Level.INFO, "  nepodařilo se zapsat info o nezpracovaném záznamu" + Utils.lineNumber());
                        } finally {
                            try {
                                if (bw != null) bw.close();
                                if (fw != null) fw.close();
                            } catch (IOException exClose) {
                                LOG.log(Level.SEVERE, " chyba při uzavírání fileWriter či BufferedWriter" + exClose.getMessage());
                            }
                        }
                    }
                }
            }
        } finally {
            oaiRecords.close();
        }

    }

    /**  Iterates records to get them persisted
     * 
     * @param library (Library)
     * @param oaiRecords (ListResult<Record>)
     * @param time (long)
     * @return (String)
     * @throws DaoException
     * @throws JAXBException
     * @throws XMLStreamException 
     */
    private String persistRecords(Library library, ListResult<Record> oaiRecords, long time)
            throws DaoException, JAXBException, XMLStreamException {
        String casPosledniZpracovanyZaznam = "";

//2017.06.26 - test zablokovani ukladani (kdyz je nastaveno na true)
//        LibraryHarvest libraryHarvest = new LibraryHarvest(library, dataSource, createMetadataParser(library), false /*conf.isDryRun()*/);
        LibraryHarvest libraryHarvest = new LibraryHarvest(library, dataSource, createMetadataParser(library), true /*conf.isDryRun()*/);
        casPosledniZpracovanyZaznam = libraryHarvest.harvest(oaiRecords, xmlCtx);
        time = System.currentTimeMillis() - time;
        LOG.log(Level.INFO, "Harvest status:\n  Records added: {0}\n  Records deleted: {1}\n  Time: {2}\n",
                new Object[]{libraryHarvest.getAddRecordCount(), libraryHarvest.getRemoveRecordCount(), Utils.elapsedTime(time)});
        return casPosledniZpracovanyZaznam;
    }

    /**
     * 
     * @param library (Library)
     * @return 
     */
    private ModsMetadataParser createMetadataParser(Library library) {
        String xslt = ModsMetadataParser.getDefaultXslt();
        return createMetadataParser(xslt);
    }

    /**
     * 
     * @param xslt (String)
     * @return 
     */
    private ModsMetadataParser createMetadataParser(String xslt) {
        ModsMetadataParser parser = parserMap.get(xslt);
        if (parser == null) {
            parser = new ModsMetadataParser(xslt);
            parserMap.put(xslt, parser);
        }
        return parser;
    }

    /**
     * 
     * @param library (Library)
     * @return (OaiSource)
     * @throws IOException 
     */
    private OaiSource resolveOaiSource(Library library) throws IOException {
        String validate = library.validate();
        if (validate != null) {
            LOG.log(Level.FINE, "{1}: {0}" + Utils.lineNumber(), new Object[]{validate, library});
            return null;
        }
        if ("oaipmh".equals(library.getHarvestProtocol())) {
            try {
                OaiSource src = oaiFactory.createListRecords(
                        library.getBaseUrl(), library.getVerbParameter(), library.getLastHarvest(), library.getFromDate(), library.getToDate(),
                        library.getMetadataFormat(), library.getQueryParameters(), null);
                return src;
            } catch (OaiException ex) {
                LOG.log(Level.WARNING, "Invalid OAI URL ''{0}''\n  {1}",
                        new Object[]{ex.getMessage(), library});
                return null;
            }
        }
        return null;
    }

    private Boolean zalozZaznam(Library library, KrameriusEntry krameriusEntry) {
        Boolean vystup = true;
        Boolean pokracuj = true;
        String idZaznamuStr = "";
        List<String> seznamHodnot = new ArrayList<String>();
        int idZaznamu;

        try {
            if (!Utils.jePrazdne(krameriusEntry.getUuid())) {
                int pocetZaznamuDigKnihovna = 0;
                Boolean nezpracovavat = false;
                Statement stmt = null;
                ResultSet rsDigKnihovnaKontrola = null;
                try {
                    stmt = this.connection.createStatement();
                    rsDigKnihovnaKontrola = stmt.
                        executeQuery("select count(*) as pocet from digobjekt where uuid='" + krameriusEntry.getUuid() + "' and ((rucnispojeni=1) or (nespojovat=1)) and rdigknihovna_digobjekt=" + krameriusEntry.getLibraryId());
                    rsDigKnihovnaKontrola.next();
                    if (rsDigKnihovnaKontrola.getInt("POCET")>0) { nezpracovavat = true; }
                } catch (SQLException ex) {
                    LOG.log(Level.SEVERE, " nepodařilo se zjistit počet záznamů, s nastavením 'ručního spojení', v DigObjekt pro uuid: " + krameriusEntry.getUuid() + " " + ex.getMessage());
                } finally {
                    Utils.tryClose(rsDigKnihovnaKontrola);
                    Utils.tryClose(stmt);
                }
                
                if (nezpracovavat) {
                    LOG.log(Level.INFO, " záznam digObjekt uuid: " + krameriusEntry.getUuid() + " má nastaveno ruční spojení či nespojovat, proto nelze smazat a regenerovat.");
                    
                    try {
                        stmt = this.connection.createStatement();
                        rsDigKnihovnaKontrola = stmt.
                            executeQuery("select * from digobjekt where uuid='" + krameriusEntry.getUuid() + "' and ((rucnispojeni=1) or (nespojovat=1)) and rdigknihovna_digobjekt=" + krameriusEntry.getLibraryId());
                        rsDigKnihovnaKontrola.next();
                        krameriusEntry.setId(rsDigKnihovnaKontrola.getString("ID"));
                        krameriusEntry.setPredlohaId(rsDigKnihovnaKontrola.getString("RPREDLOHA_DIGOBJEKT"));
                    } catch (SQLException ex) {
                        LOG.log(Level.SEVERE, " nepodařilo se zjistit počet záznamů, s nastavením 'ručního spojení', v DigObjekt pro uuid: " + krameriusEntry.getUuid() + " " + ex.getMessage());
                    } finally {
                        Utils.tryClose(rsDigKnihovnaKontrola);
                        Utils.tryClose(stmt);
                    }
                    
                } else {
                    pocetZaznamuDigKnihovna = 0;
                    try {
                        stmt = this.connection.createStatement();
                        rsDigKnihovnaKontrola = stmt.
                            executeQuery("select count(*) as pocet from digobjekt where uuid='" + krameriusEntry.getUuid() + "' and rdigknihovna_digobjekt=" + krameriusEntry.getLibraryId());
                        rsDigKnihovnaKontrola.next();
                        pocetZaznamuDigKnihovna = rsDigKnihovnaKontrola.getInt("POCET");
                    } catch (SQLException ex) {
                        LOG.log(Level.SEVERE, " nepodařilo se zjistit počet záznamů v DigObjekt pro uuid: " + krameriusEntry.getUuid() + " " + ex.getMessage());
                    } finally {
                        Utils.tryClose(rsDigKnihovnaKontrola);
                        Utils.tryClose(stmt);
                    }
                    PreparedStatement pstmtDigObjekt = null;
                    PreparedStatement pstmtMetadata = null;
                    ResultSet rsDigObjekt = null;
                    ResultSet rsMetadata = null;
                    try {
                        stmt = this.connection.createStatement();
                        if (pocetZaznamuDigKnihovna>0) {
                            String podminkaDigObjekt = " where uuid='" + krameriusEntry.getUuid() + "' and (rucnispojeni is null or rucnispojeni=0) and rdigknihovna_digobjekt=" + krameriusEntry.getLibraryId();
                            String podminkaMetadata = "";

                            rsDigObjekt = stmt.executeQuery("select LISTAGG(id, ',') WITHIN GROUP (ORDER BY id) as ids from digobjekt " + podminkaDigObjekt);
                            LOG.log(Level.INFO, "sql: " + "select LISTAGG(id, ',') WITHIN GROUP (ORDER BY id) as ids from digobjekt " + podminkaDigObjekt);
                            rsDigObjekt.next();
                            String nactenaId = rsDigObjekt.getString("IDS").replace(",","','");
                            podminkaMetadata = " where rdigobjekt_metadata in ('" + nactenaId + "')";
                            podminkaDigObjekt = " where id in ('" + nactenaId + "')";
                            //bohužel to nejde takto jednoduše, takže info o vymazaných záznamech vynecháme
                            //stmt.execute("select * into metadata_smazano from metadata " + podminkaMetadata);
                            //stmt.execute("select * into digobjekt_smazano from digobjekt " + podminkaDigObjekt);
                            //konec bohužel.....
                            stmt.execute("delete metadata " + podminkaMetadata);
                            LOG.log(Level.INFO, "mažu: " + "delete metadata " + podminkaMetadata);
                            stmt.execute("delete digobjekt " + podminkaDigObjekt);
                            LOG.log(Level.INFO, " mažu: " + "delete digobjekt " + podminkaDigObjekt);
                        }

                        pstmtDigObjekt = this.connection.prepareStatement(
                            "insert into DIGOBJEKT"
                            + " (ID, UUID, DRUHDOKUMENTU, JSON, ZALDATE, EDIDATE, RDIGKNIHOVNA_DIGOBJEKT)"
                            + " values (?, ?, ?, ?, sysdate, sysdate, ?)");
                        pstmtMetadata = this.connection.prepareStatement(
                            "insert into METADATA"
                            + " (ID, RELIEFNAME, VALID, VALUE, RDIGOBJEKT_METADATA)"
                            + " values (?, ?, ?, ?, ?)");

                        idZaznamuStr = vratId("DigObjekt");
                        krameriusEntry.setId(idZaznamuStr);
                        if (!"".equals(idZaznamuStr)) {
                            idZaznamu = 0;
                            try {
                                if (pocetZaznamuDigKnihovna==0) {
                                    idZaznamu = Integer.decode(idZaznamuStr);
                                    pstmtDigObjekt.setInt(1, idZaznamu);
                                    pstmtDigObjekt.setString(2, krameriusEntry.getUuid());
                                    pstmtDigObjekt.setString(3, krameriusEntry.getDruhDokumentu());
                                    pstmtDigObjekt.setString(4, Utils.toJson(krameriusEntry));
                                    pstmtDigObjekt.setBigDecimal(5, library.getId());
                                }
                                if (this.zapisDoDatabaze) {
                                    pstmtDigObjekt.execute();
                                    LOG.log(Level.INFO, " sql DigObjekt: " + ((oracle.jdbc.driver.OraclePreparedStatement) pstmtDigObjekt).getOriginalSql().replace(" values (?, ?, ?, ?, sysdate, sysdate, ?)", " ") + " values (" + idZaznamu + ", " + krameriusEntry.getUuid() + ", " + krameriusEntry.getDruhDokumentu() + ", " + Utils.toJson(krameriusEntry) + ", " + library.getId() + "); \n");
                                    LOG.log(Level.INFO, " záznam digObjekt založen - pokračuj: " + pokracuj);
                                } else {
                                    bwSqlPrikazy.write("digObjekt: \n");
                                    bwSqlPrikazy.write("  " + ((oracle.jdbc.driver.OraclePreparedStatement) pstmtDigObjekt).getOriginalSql().replace(" values (?, ?, ?, ?, sysdate, sysdate, ?)", " ") + " values (" + idZaznamu + ", '" + krameriusEntry.getUuid() + "', '" + krameriusEntry.getDruhDokumentu() + "', '" + /*Utils.toJson(krameriusEntry) +*/ "', sysdate, sysdate, " + library.getId() + "); \n");
                                    bwSqlPrikazy.write(" metadata: \n");
                                }

                               // zakládám záznamy v Metadata 
                                zpracujMetadataList(pstmtMetadata, "urnnbn", krameriusEntry.getUrnnbn(), idZaznamu, 1);
                               //autor
                                List<AutorEntry> autorList = krameriusEntry.getAutor();
                                for (int i=0; i<autorList.size(); i++) {
                                    zpracujMetadata(pstmtMetadata, "autor", autorList.get(i).getFullAutor(), idZaznamu, 1);
                                }
                               //autor konec
                                zpracujMetadata(pstmtMetadata, "siglaBibUdaju", krameriusEntry.getSiglaBibUdaju(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "cislo", krameriusEntry.getCisloPeriodika(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "siglaFyzJednotky", krameriusEntry.getSigla(), idZaznamu, 1);
                               //nazev
                                zpracujMetadata(pstmtMetadata, "nazev", krameriusEntry.getIssueTitle(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "nazev", krameriusEntry.getVolumeTitle(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "nazev", krameriusEntry.getUnitTitle(), idZaznamu, 1);
                                List<NazevEntry> nazevList = krameriusEntry.getNazev();
                                for (int i=0; i<nazevList.size(); i++) {
                                    zpracujMetadata(pstmtMetadata, "nazev", nazevList.get(i).getFullTitle(), idZaznamu, 1);
                                }
                               //nazev konec
                                zpracujMetadata(pstmtMetadata, "barcode", krameriusEntry.getCarKod(), idZaznamu, 1);
                                zpracujMetadataList(pstmtMetadata, "rokVydani", krameriusEntry.getDatumVydani(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "katalog", krameriusEntry.getKatalog(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "pole001", krameriusEntry.getPole001(), idZaznamu, 1);
                                zpracujMetadata(pstmtMetadata, "signatura", krameriusEntry.getSignatura(), idZaznamu, 1);
                                zpracujMetadataList(pstmtMetadata, "isbn", krameriusEntry.getIsbn(), idZaznamu, 1);
                                zpracujMetadataList(pstmtMetadata, "isbn", krameriusEntry.getNepIsbn(), idZaznamu, 0);
                                zpracujMetadataList(pstmtMetadata, "issn", krameriusEntry.getIssn(), idZaznamu, 1);
                                zpracujMetadataList(pstmtMetadata, "issn", krameriusEntry.getNepIssn(), idZaznamu, 0);
                                zpracujMetadataList(pstmtMetadata, "ccnb", krameriusEntry.getCcnb(), idZaznamu, 1);
                                zpracujMetadataList(pstmtMetadata, "ccnb", krameriusEntry.getNepCcnb(), idZaznamu, 0);
                                zpracujMetadataList(pstmtMetadata, "oclc", krameriusEntry.getOclc(), idZaznamu, 1);

                                if (this.zapisDoDatabaze) {
                                    LOG.log(Level.INFO, " potvrzuji změny - commit");
                                    this.connection.commit();
                                }
                                krameriusEntry.setId("" + idZaznamu);
                            } catch (Exception ex) {
                                System.out.println(" chyba : " + ex.getMessage());
                                if (this.zapisDoDatabaze) { this.connection.rollback(); }
                                vystup = false;
                            } finally {
                                Utils.tryClose(pstmtMetadata);
                                Utils.tryClose(pstmtDigObjekt);
                            }         
                        }         
                    } catch (SQLException ex) {
                        String pomocnyText = "";
                        if (pocetZaznamuDigKnihovna==0) { pomocnyText = "založit"; } else { pomocnyText = "upravit"; }
                        LOG.log(Level.SEVERE, " nepodařilo se " + pomocnyText + " záznam " + ex.getMessage());
                    } finally {
                        Utils.tryClose(rsMetadata);
                        Utils.tryClose(rsDigObjekt);
                        Utils.tryClose(pstmtMetadata);
                        Utils.tryClose(pstmtDigObjekt);
                        Utils.tryClose(stmt);
                    }
                }
            } else {
                LOG.log(Level.INFO, " zázname nemá vyplněn uuid");
            }
            
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, " chyba při zakládání/editaci záznamu " + ex.getMessage());
        }
        return vystup;
    }
    
    private Boolean zpracujMetadata(PreparedStatement pstmtMetadata, String reliefName, String value, int idZaznamuDigObjekt, int valid) {
        Boolean vystup = true;
        String idZaznamuStr = "";
        int idZaznamu;
        if (!"".equals(value)) {
            idZaznamuStr = vratId("Metadata");
            if (!"".equals(idZaznamuStr)) {
                idZaznamu = Integer.decode(idZaznamuStr);
                LOG.log(Level.INFO, " idZaznamu (Metadata): " + idZaznamu);
                try {
                    pstmtMetadata.setInt(1, idZaznamu);
                    pstmtMetadata.setString(2, reliefName);
                    pstmtMetadata.setInt(3, valid);
                    pstmtMetadata.setString(4, value);
                    pstmtMetadata.setInt(5, idZaznamuDigObjekt);
                    if (this.zapisDoDatabaze) {
                        pstmtMetadata.execute();
                        LOG.log(Level.INFO, " " + ((oracle.jdbc.driver.OraclePreparedStatement) pstmtMetadata).getOriginalSql().replace(" values (?, ?, ?, ?, ?)", " ") + " values (" + idZaznamu + ", '" + reliefName + "', " + valid + ", '" + value + "', " + idZaznamuDigObjekt + "); \n");
                    } else {
                        bwSqlPrikazy.write("  " + ((oracle.jdbc.driver.OraclePreparedStatement) pstmtMetadata).getOriginalSql().replace(" values (?, ?, ?, ?, ?)", " ") + " values (" + idZaznamu + ", '" + reliefName + "', " + valid + ", '" + value + "', " + idZaznamuDigObjekt + "); \n");
                    }

                } catch (SQLException ex) {
                    LOG.log(Level.SEVERE, " chyba při zpracování SQL příkazu " + ex.getMessage());
                    vystup = false;
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, " chyba při tvorbě souboru s SQL příkazy " + ex.getMessage());
                    vystup = false;
                }
            }
        }
        return vystup;
    }
    
    private Boolean zpracujMetadataList(PreparedStatement pstmtMetadata, String reliefName, List<String> seznamHodnot, int idZaznamuNadrizene, int valid) {
        Boolean pokracuj = true;
        for (int i=0; i<seznamHodnot.size(); i++) {
            if (!"".equals(seznamHodnot.get(i))) {
                zpracujMetadata(pstmtMetadata, reliefName, seznamHodnot.get(i), idZaznamuNadrizene, valid);
            }
        }
        return pokracuj;
    }
    
    private String vratId(String tabulka) {
        String vystup = "";
        Connection connectionLocal = null;
        PreparedStatement statement1 = null;
        PreparedStatement statement2 = null;
        ResultSet rs = null;
        try {
            connectionLocal = this.dataSource.getConnection();
            Boolean opakuj = true;
            int idZaznamu = 0;
            int idZaznamuLoc = 0;
            statement1 = connectionLocal.prepareStatement("select id from plaant_ids where deskname='cz.incad.rd." + tabulka + "'");
            statement2 = connectionLocal.prepareStatement("update plaant_ids set id=id+1 where deskname='cz.incad.rd." + tabulka + "'");
            while (opakuj) {
                rs = statement1.executeQuery();
                rs.next();
                idZaznamu = rs.getInt("ID");
                statement2.execute();
                rs = statement1.executeQuery();
                rs.next();
                idZaznamuLoc = rs.getInt("ID");
                if (idZaznamu+1==idZaznamuLoc) {
                    opakuj = false;
                    vystup = "" + idZaznamuLoc;
                }
            }
        } catch (SQLException ex) {
            LOG.log(Level.SEVERE, "chyba v SQL příkazu " + ex.getMessage());
        } finally {
            Utils.tryClose(rs);
            Utils.tryClose(statement2);
            Utils.tryClose(statement1);
            Utils.tryClose(connectionLocal);
            
        }
        
        return vystup;
    }
    
//    private Boolean pripojPredlohu(Library library, KrameriusEntry krameriusEntry) {
    private Boolean pripojPredlohu(KrameriusEntry krameriusEntry) {
        Boolean vystup = false;
        PreparedStatement pstmtDigObjekt = null;
        try {
            String idZaznamuStr = krameriusEntry.getId();
            if (!Utils.jePrazdne(idZaznamuStr)) {
                pstmtDigObjekt = this.connection.prepareStatement(
                        "select rpredloha_digObjekt from digObjekt where id=" + idZaznamuStr
                );
                ResultSet rs = pstmtDigObjekt.executeQuery();
                rs.next();
                String idPredlohaStr = rs.getString("RPREDLOHA_DIGOBJEKT");
                if (Utils.jePrazdne(idPredlohaStr)) { vystup = HledejPredlohu(krameriusEntry); }
            }
            
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, " chyba " + ex.getMessage());
        } finally {
            Utils.tryClose(pstmtDigObjekt);
        }
        return vystup;
    }
    
//    private Boolean HledejPredlohu(Library library, KrameriusEntry krameriusEntry) {
    private Boolean HledejPredlohu(KrameriusEntry krameriusEntry) {
        Boolean vystup = true;
        String druhDokumentu = krameriusEntry.getDruhDokumentu();
        if (("MONOGRAPH".equals(druhDokumentu)) || ("MONOGRAPHUNIT".equals(druhDokumentu))) {
            LOG.log(Level.INFO, " hledám monografii");
            vystup = HledejMonografii(krameriusEntry);
        } else if ("PERIODICAL".equals(druhDokumentu)) {
            LOG.log(Level.INFO, " hledám periodikum");
            vystup = HledejPeriodikum(krameriusEntry);
        }
        return vystup;
    }
    
//    private Boolean HledejMonografii(Library library, KrameriusEntry krameriusEntry) {
    private Boolean HledejMonografii(KrameriusEntry krameriusEntry) {
        LOG.log(Level.INFO, " metoda: HledejMonografii");
        HashMap<String, String> hMap = new HashMap<String, String>();
        Boolean vystup = false;
        // první hledání podle sigla1 a pole001
        String dalsiPodminkaZaklad = " sigla1='" + krameriusEntry.getSigla()+ "' and pole001='" + krameriusEntry.getPole001() + "'";
        String dalsiPodminka = dalsiPodminkaZaklad;
        String pomocnyVystup = "";
        Boolean pokracuj = true;
        hMap = HledejMonografiiHledani(krameriusEntry, dalsiPodminka);
        dalsiPodminka = hMap.get("podminka");
        if ("1".equals(hMap.get("pocet"))) { pokracuj = false; }
        //pokud nebyl nalezen žádný záznam a dodmínka je stejná jako na začátku, pak hledat ve všech záznamech knihovny (bez pole001)
        if ((pokracuj) && (dalsiPodminkaZaklad.equals(dalsiPodminka)) && (("0".equals(hMap.get("pocet"))) || (Utils.jePrazdne(hMap.get("pocet"))))) {
            //hledání jen ve vlastní knihovně
            dalsiPodminkaZaklad = " sigla1='" + krameriusEntry.getSigla()+ "'";
            dalsiPodminka = dalsiPodminkaZaklad;
            hMap = HledejMonografiiHledani(krameriusEntry, dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) { pokracuj = false; }
            //pokud nebyl nalezen žádný záznam, hledá se i v ostatních knihovnách

            ///* -- toto je dočasně zablokováno, hledání v cizích knihovnách na základě emailu z 2018.03.14 - p. Dvořáková
            if ((pokracuj) && ("0".equals(hMap.get("pocet")))) {
                hMap = HledejMonografiiHledani(krameriusEntry, "");
                if ("1".equals(hMap.get("pocet"))) { 
                    pokracuj = false;
                    hMap.put("spojeno3kolo","1");
                }
            }
            //*/
        }

        vystup = VytvorZaznamDoSouboru(pokracuj, hMap, krameriusEntry, 
                "bwSouborProNalezeneZaznamyMonografie", "bwSouborProNenalezeneZaznamyMonografie", 
                "bwSouborProChybneZaznamyMonografie", "bwSouborProNejednoznacneZaznamyMonografie");

        return vystup;
    }

    private HashMap<String, String> HledejMonografiiHledani(KrameriusEntry krameriusEntry, String dalsiPodminka) {
        LOG.log(Level.INFO, " metoda: HledejMonografiiHledani");
        HashMap<String, String> hMap = new HashMap<String, String>();
        Boolean pokracuj = true;
        if (!Utils.jePrazdne(krameriusEntry.getCarKod())) {
            hMap = HledejZaznamPredlohy("carkod", krameriusEntry.getCarKod(), dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) {
                pokracuj = false;
            } else if (Utils.parseInteger(hMap.get("pocet"))>0) {
                if (!Utils.jePrazdne(hMap.get("podminka"))) dalsiPodminka = hMap.get("podminka");
            }
        }
        if ((pokracuj) && (!Utils.jePrazdne(krameriusEntry.getSignatura()))) {
            hMap = HledejZaznamPredlohy("signatura", krameriusEntry.getSignatura(), dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) {
                pokracuj = false;
            } else if (Utils.parseInteger(hMap.get("pocet"))>0) {
                if (!Utils.jePrazdne(hMap.get("podminka"))) dalsiPodminka = hMap.get("podminka");
/* 
    -- po dohodě tato možnost zablokována: email z 2018.03.19-13:14 p. Dvořáková. Zprovozněno v cca 11:00 stejného dne pouze pro testy
    -- nikdy nenasazeno na ostrý provoz. Zde ponecháno pro případ opětovného požadavku na zprovoznění                 
            } else if (Utils.parseInteger(hMap.get("pocet"))==0) {
                String signaturaPomocna = krameriusEntry.getSignatura();
                String hledanyText = "/Přív.";
                if (signaturaPomocna.contains(hledanyText)) {
                    signaturaPomocna = signaturaPomocna.substring(0, signaturaPomocna.indexOf(hledanyText));
                    HashMap<String, String> hMapPom = hMap;
                    hMapPom = HledejZaznamPredlohy("signatura", signaturaPomocna, dalsiPodminka);
                    if ("1".equals(hMapPom.get("pocet"))) {
                        hMap = hMapPom;
                        pokracuj = false;
                    }
                }
*/
            }
        }
        if ((pokracuj) && (!Utils.jePrazdne(krameriusEntry.getCcnb()))) {
            hMap = HledejZaznamPredlohy("ccnb",  Utils.ListToString(krameriusEntry.getCcnb()), dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) {
                pokracuj = false;
            } else if (Utils.parseInteger(hMap.get("pocet"))>0) {
                if (!Utils.jePrazdne(hMap.get("podminka"))) dalsiPodminka = hMap.get("podminka");
            }
        }
        if ((pokracuj) && (!Utils.jePrazdne(krameriusEntry.getIsbn()))) {
            hMap = HledejZaznamPredlohy("isbn", Utils.ListToString(krameriusEntry.getIsbn()), dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) {
                pokracuj = false;
            } else if (Utils.parseInteger(hMap.get("pocet"))>0) {
                if (!Utils.jePrazdne(hMap.get("podminka"))) dalsiPodminka = hMap.get("podminka");
            }
        }
        return hMap;
    }
           
//    private Boolean HledejPeriodikum(Library library, KrameriusEntry krameriusEntry) {
    private Boolean HledejPeriodikum(KrameriusEntry krameriusEntry) {
        HashMap<String, String> hMap = new HashMap<String, String>();
        Boolean vystup = false;
        // první hledání je ve vlastní knihovně, spolus s pole001
        String dalsiPodminkaZaklad = " sigla1='" + krameriusEntry.getSigla()+ "' and pole001='" + krameriusEntry.getPole001() + "'";
        String dalsiPodminka = dalsiPodminkaZaklad;
        String pomocnyVystup = "";
        Boolean pokracuj = true;
        
        hMap = HledejPeriodikumHledani(krameriusEntry, dalsiPodminka);
        dalsiPodminka = hMap.get("podminka");
        if ("1".equals(hMap.get("pocet"))) { pokracuj = false; }
        //pokud nebyl nalezen žádný záznam a dodmínka je stejná jako na začátku = nenalezen žádný záznam
        //  -> pak hledat ve všech záznamech (bez pole001)
        if ((pokracuj) && (dalsiPodminkaZaklad.equals(dalsiPodminka)) && (("0".equals(hMap.get("pocet"))) || (Utils.jePrazdne(hMap.get("pocet"))))) {
            //hledání jen ve vlastní knihovně
            dalsiPodminkaZaklad = " sigla1='" + krameriusEntry.getSigla()+ "'";
            dalsiPodminka = dalsiPodminkaZaklad;
            hMap = HledejPeriodikumHledani(krameriusEntry, dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) { pokracuj = false; }
            //pokud nebyl nalezen žádný záznam, hledá se i v ostatních knihovnách

            ///* -- toto je dočasně zablokováno, hledání v cizích knihovnách na základě emailu z 2018.03.14 - p. Dvořáková
            if ((pokracuj) && ("0".equals(hMap.get("pocet")))) {
                hMap = HledejPeriodikumHledani(krameriusEntry, "");
                if ("1".equals(hMap.get("pocet"))) { 
                    pokracuj = false;
                    hMap.put("spojeno3kolo","1");
                }
            }
            //*/
        }

        vystup = VytvorZaznamDoSouboru(pokracuj, hMap, krameriusEntry,
                "bwSouborProNalezeneZaznamyPeriodika", "bwSouborProNenalezeneZaznamyPeriodika", 
                "bwSouborProChybneZaznamyPeriodika", "bwSouborProNejednoznacneZaznamyPeriodika");

        return vystup;
    }

    private HashMap<String, String> HledejPeriodikumHledani(KrameriusEntry krameriusEntry, String dalsiPodminka) {
        LOG.log(Level.INFO, " metoda: HledejPeriodikumHledani");
        HashMap<String, String> hMap = new HashMap<String, String>();
        Boolean pokracuj = true;
        
        /* -- spojování periodik je momentálně nezapojené!!!!! - email z 2018.03.15 p. Dvořáková
        if ((pokracuj) && (!Utils.jePrazdne(krameriusEntry.getIssn()))) {
            hMap = HledejZaznamPredlohy("issn", Utils.ListToString(krameriusEntry.getIssn()), dalsiPodminka);
            if ("1".equals(hMap.get("pocet"))) {
                pokracuj = false;
            } else if ((Utils.parseInteger(hMap.get("pocet"))>0) {
                if (!Utils.jePrazdne(hMap.get("podminka"))) dalsiPodminka = hMap.get("podminka");
            }
        }
        */
        
        return hMap;
    }
    
    private Boolean VytvorZaznamDoSouboru(Boolean nespojenyZaznam, HashMap<String, String> hMap, KrameriusEntry krameriusEntry, 
            String souborProNalezeneZaznamy, String souborProNenalezeneZaznamy, 
            String souborProChybneZaznamy, String souborProNejednoznacneZaznamy) {
        Boolean vystup = false;

        // kontrola na počty záznamů, pro případ kdyby to neodpovídalo
        if ((Utils.parseInteger(hMap.get("pocet"))==0) && (!nespojenyZaznam)) {
            LOG.log(Level.INFO, " chybné nastavení parametrů - přenastavuji =0 - " + Utils.parseInteger(hMap.get("pocet")) + " " + nespojenyZaznam);
            nespojenyZaznam = true;
        }
        if ((Utils.parseInteger(hMap.get("pocet"))>1) && (!nespojenyZaznam)) {
            LOG.log(Level.INFO, " chybné nastavení parametrů - přenastavuji >1 - " + Utils.parseInteger(hMap.get("pocet")) + " " + nespojenyZaznam);
            nespojenyZaznam = true;
        }
        if ((Utils.parseInteger(hMap.get("pocet"))==1) && (nespojenyZaznam)) { 
            LOG.log(Level.INFO, " chybné nastavení parametrů - přenastavuji =1 - " + Utils.parseInteger(hMap.get("pocet")) + " " + nespojenyZaznam);
            nespojenyZaznam = false; 
        }
        // konec kontroly na počty záznamů
        
        if (nespojenyZaznam) {
            try {
                if (Utils.parseInteger(hMap.get("pocet"))==0) {
                    zapisDoSouboru(
                            souborProNenalezeneZaznamy,
                            getIdentifikaceZaznamuProSoubor(krameriusEntry),
                            "záznam nebyl nalezen");
                } else {
                    zapisDoSouboru(
                            souborProNejednoznacneZaznamy, 
                            getIdentifikaceZaznamuProSoubor(krameriusEntry) + ";(" + hMap.get("idcisla") + ")",
                            "nejednoznačný záznam");
                }
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, " chyba " + ex.getMessage());
            }
        } else {
            if ("1".equals(hMap.get("pocet"))) {
                LOG.log(Level.INFO, " našel jsem záznam");

                ResultSet rs = null;
                ResultSet rs3 = null;
                Statement statement = null;
                try {
                    statement = this.connection.createStatement();
                    rs = statement.executeQuery("select id from PREDLOHA where " + hMap.get("podminka"));
                    rs.next();
                    String idZaznamu = rs.getString("ID");
                    if (!Utils.jePrazdne(idZaznamu)) {
                        String pomocnyUpdate = "";
                        String pomocnePole = "";
                        if ("1".equals(hMap.get("spojeno3kolo"))) { 
                            pomocnyUpdate = ", kolo3=1"; 
                            pomocnePole = "@";
                        }
                        sqlPrikazDocasnyText = "update DIGOBJEKT set RPREDLOHA_DIGOBJEKT = " + idZaznamu + pomocnyUpdate + " where uuid='" + krameriusEntry.getUuid() + "'";
                        LOG.log(Level.INFO, " update DigObjektu: " + sqlPrikazDocasnyText);
                        Boolean provedeno = false;
                        if (zapisDoDatabaze) {
                            statement.execute(sqlPrikazDocasnyText);
                        } else {
                            zapisDoSouboru(
                                    "bwSqlPrikazy",
                                    sqlPrikazDocasnyText,
                                    "update DigObjekt");
                        }

                        sqlPrikazDocasnyText = "select count(*) as pocet from DIGOBJEKT where RPREDLOHA_DIGOBJEKT = " + idZaznamu + " and uuid='" + krameriusEntry.getUuid() + "' and rdigknihovna_digobjekt=" + krameriusEntry.getLibraryId();
                        LOG.log(Level.INFO, " select změněného DigObjektu: " + sqlPrikazDocasnyText);
                        rs3 = statement.executeQuery(sqlPrikazDocasnyText);
                        rs3.next();
                        if ("1".equals(rs3.getString("POCET"))) { provedeno = true; }
                        if (provedeno) {
                            if (zapisDoDatabaze) { this.connection.commit(); }
                            String pole001Str = hMap.get("pole001");
                            String sigla1Str = hMap.get("sigla1");
                            zapisDoSouboru(
                                    souborProNalezeneZaznamy,
                                    pole001Str + ";" + sigla1Str + ";" + krameriusEntry.getUuid() + ";" + pomocnePole,
                                    " úspěšný zápis");
                            vystup = true;
                        } else {
                            zapisDoSouboru(
                                    souborProChybneZaznamy, 
                                    getIdentifikaceZaznamuProSoubor(krameriusEntry),
                                    " nepovedlo se upravit záznam");
                            if (zapisDoDatabaze) { this.connection.rollback(); }
                        }
                    } else {
                        LOG.log(Level.INFO, " chyba: nepovedlo se najít záznam v předloze, ačkoliv by měl existovat");
                        zapisDoSouboru(
                                souborProNenalezeneZaznamy, 
                                getIdentifikaceZaznamuProSoubor(krameriusEntry),
                                " nepovedlo se najít záznam v předloze, ačkoliv by měl existovat");
                    }
                } catch (SQLException ex) {
                    LOG.log(Level.INFO, " zápis kvůli chybě");
                    zapisDoSouboru(
                            souborProChybneZaznamy, 
                            getIdentifikaceZaznamuProSoubor(krameriusEntry),
                            " SQL chyba");
                    try {
                        this.connection.rollback();
                    } catch (SQLException ex1) {
                        LOG.log(Level.SEVERE, " chyba při rollbacku zaznamu" + Utils.lineNumber());
                    }
                } catch (Exception ex) {
                    LOG.log(Level.INFO, " zápis kvůli chybě");
                    zapisDoSouboru(
                            souborProChybneZaznamy,
                            getIdentifikaceZaznamuProSoubor(krameriusEntry),
                            " obecná chyba ");
                    try {
                        this.connection.rollback();
                    } catch (SQLException ex1) {
                        LOG.log(Level.SEVERE, " chyba  pri rollbacku zaznamu" + Utils.lineNumber());
                    }
                } finally {
                    Utils.tryClose(rs3);
                    Utils.tryClose(rs);
                    Utils.tryClose(statement);
                }
            } else { //toto by nemělo nastat, je to zde jen pro jistotu
                zapisDoSouboru(
                        souborProNejednoznacneZaznamy,
                        getIdentifikaceZaznamuProSoubor(krameriusEntry),
                        " vráceno více než 1 záznam (SpojPredlohaDigObjekt) ");
            }
        }
    
        return vystup;
    }
    
    private String getIdentifikaceZaznamuProSoubor(KrameriusEntry krameriusEntry) {
        return krameriusEntry.getUuid()
                + ";" + krameriusEntry.getPole001() + ";" + krameriusEntry.getCarKod()
                + ";" + krameriusEntry.getSignatura() + ";" + krameriusEntry.getCcnb(0)
                + ";" + krameriusEntry.getIssn(0);
    }
    
    private HashMap<String, String> HledejZaznamPredlohy(String pole, String hodnoty, String dalsiPodminka) {
        //LOG.log(Level.INFO, " metoda: HledejZaznamPredlohy - " + dalsiPodminka);
        String podminkaVstup = dalsiPodminka;
        String podminkaVystup = podminkaVstup;
        HashMap<String, String> hMap = new HashMap<String, String>();
        if (!Utils.jePrazdne(dalsiPodminka)) {
            if (!"and ".equals(dalsiPodminka.trim().substring(0, 4))) { dalsiPodminka = " and " + dalsiPodminka; }
        } else {
            dalsiPodminka = "";
        }
        podminkaVystup = "(" + pole + " in ('" + hodnoty + "'))" + dalsiPodminka;
        String sql = "select count(*) as pocet, LISTAGG(idcislo, ',') WITHIN GROUP (ORDER BY idcislo) as idcisla from PREDLOHA where " + podminkaVystup;
        LOG.log(Level.INFO, " sql: " + sql);
        Statement statement = null;
        ResultSet rs = null;
        ResultSet rs2 = null;
        try {
            statement = this.connection.createStatement();
            rs = statement.executeQuery(sql);
            if (rs.next()) {
                String pocetZaznamu = rs.getString("POCET");
                LOG.log(Level.INFO, " počet nalezených záznamů: " + pocetZaznamu);
                hMap.put("pocet",pocetZaznamu);
                if ("0".equals(pocetZaznamu)) {
                    hMap.put("podminka",podminkaVstup);
                } else if ("1".equals(pocetZaznamu)) {
                    hMap.put("podminka",podminkaVystup);
                    rs2 = statement.executeQuery("select * from PREDLOHA where " + podminkaVystup);
                    LOG.log(Level.INFO, "sql: " + "select * from PREDLOHA where " + podminkaVystup);
                    if (rs2.next()) {
                        hMap.put("pole001", rs2.getString("POLE001"));
                        hMap.put("sigla1", rs2.getString("SIGLA1"));
                        LOG.log(Level.INFO, " pole001: " + hMap.get("pole001") + " -- sigla1: " + hMap.get("sigla1"));
                    }
                } else {
                    hMap.put("podminka",podminkaVystup);
                    hMap.put("idcisla",rs.getString("IDCISLA"));
                }
            }
        } catch (Exception ex) {
            LOG.log(Level.INFO, " chyba při zjišťování počtu záznamů (HledejZaznamPredlohy): ");
        } finally {
            Utils.tryClose(rs2);
            Utils.tryClose(rs);
            Utils.tryClose(statement);
        }
        
        //LOG.log(Level.INFO, " kontrolní počet záznamů při opuštění procedury: " + hMap.get("pocet"));
        return hMap;
    }
    
    private Boolean zapisDoSouboru(BufferedWriter soubor, String souborStr, String hodnota, String popisStr) {
        Boolean vystup = true;
        try {
            if (soubor == null) {
                LOG.log(Level.SEVERE, "Soubor pro zápis: " + souborStr + " není definován");
            } else {
                soubor.write("" + hodnota + "\n");
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Nepodařilo se zapsat do souboru: " + souborStr + ", hodnota: " + hodnota + ", popis: " + popisStr);
            vystup = false;
        }
        return vystup;
    }

    private Boolean zapisDoSouboru(String souborStr, String hodnota, String popisStr) {
        BufferedWriter soubor = null;
        switch (souborStr) {
            case "bwSouborProNenalezeneZaznamyMonografie": 
                soubor = this.bwSouborProNenalezeneZaznamyMonografie;
                break;
            case "bwSouborProNalezeneZaznamyMonografie": 
                soubor = this.bwSouborProNalezeneZaznamyMonografie;
                break;
            case "bwSouborProChybneZaznamyMonografie": 
                soubor = this.bwSouborProChybneZaznamyMonografie;
                break;
            case "bwSouborProNejednoznacneZaznamyMonografie": 
                soubor = this.bwSouborProNejednoznacneZaznamyMonografie;
                break;
            case "bwSouborProNenalezeneZaznamyPeriodika": 
                soubor = this.bwSouborProNenalezeneZaznamyPeriodika;
                break;
            case "bwSouborProNalezeneZaznamyPeriodika": 
                soubor = this.bwSouborProNalezeneZaznamyPeriodika;
                break;
            case "bwSouborProChybneZaznamyPeriodika": 
                soubor = this.bwSouborProChybneZaznamyPeriodika;
                break;
            case "bwSouborProNejednoznacneZaznamyPeriodika": 
                soubor = this.bwSouborProNejednoznacneZaznamyPeriodika;
                break;
            case "bwSqlPrikazy": 
                soubor = this.bwSqlPrikazy;
                break;
            default:
                soubor = null;
                break;
        }
        
        Boolean vystup = true;
        try {
            if (soubor == null) {
                LOG.log(Level.SEVERE, "Soubor pro zápis: " + souborStr + " není definován");
            } else {
                if (hodnota != null) {
                    soubor.write("" + hodnota + "\n");
                    soubor.flush();
                }
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Nepodařilo se zapsat do souboru: " + souborStr + ", hodnota: " + hodnota + ", popis: " + popisStr);
            vystup = false;
        }
        return vystup;
    }

    //@JsonIgnoreProperties(ignoreUnknown = true)
    private void najitPredlohyBezDigObjektu() {
        Statement stmt = null;
        Statement stmt2 = null;
        ResultSet rs = null;
        ResultSet rs2 = null;
        int zpracovavanyZaznam = 0;
        Boolean zalozenaVlastniTransakce = false;
        
        if (this.connection == null) {
            zalozenaVlastniTransakce = true;
            LOG.log(Level.INFO, " zakládám vlastní transakci a připojení");
            try {
                this.connection = dataSource.getConnection();
                LOG.log(Level.INFO, " connection zahájena");
            } catch (SQLException x) {
                LOG.log(Level.SEVERE, " chyba při tvorbě connection");
            }
            
        }

        try {
            stmt = this.connection.createStatement();
            stmt2 = this.connection.createStatement();
            // dočasně jen po jednom záznamu z každé skupiny
//            String sqlDotaz = "select id from digObjekt where id in (954203, 1070797, 1070798)";
            String sqlDotaz = "select id from digObjekt where (json is not null or xml is not null) and rpredloha_digobjekt is null and (nespojovat is null or nespojovat=0)";
            LOG.log(Level.INFO, " sql: " + sqlDotaz);
            rs = stmt.executeQuery(sqlDotaz);
//            rs = stmt.executeQuery("select id from digObjekt where xml is not null and rpredloha_digobjekt is null");
            String idPredlohaStr = "";

            while (rs.next()) {
                idPredlohaStr = rs.getString("ID");
                sqlDotaz = "select xml, json, length(json) as length_json, length(xml) as length_xml, "
                        + "uuid, druhDokumentu, rdigknihovna_digobjekt from digobjekt where id=" + idPredlohaStr;
                LOG.log(Level.INFO, " sql" + rs.getRow() + " : " + sqlDotaz);
                rs2 = stmt2.executeQuery(sqlDotaz);
                rs2.next();
                String jsonData = "";
                String xmlData = "";
                KrameriusEntry krameriusEntry = new KrameriusEntry();
//                ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                ObjectMapper mapper = new ObjectMapper();
                if (Utils.parseInteger(rs2.getString("LENGTH_JSON"))>0) {
                    LOG.log(Level.INFO, " zpracovávám json");
                    jsonData = rs2.getString("JSON");
                    try {
                        krameriusEntry = mapper.readValue(jsonData, KrameriusEntry.class);
                    } catch (IOException e) {
                        LOG.log(Level.SEVERE, " chyba při parsování z json do krameriusEntry" + e.getMessage());
                    }
                    LOG.log(Level.INFO, " json zpracován úspěšně");
                } else if (Utils.parseInteger(rs2.getString("LENGTH_XML"))>0) {
                    LOG.log(Level.INFO, " zpracovávám xml");
                    xmlData = rs2.getString("XML"); 

                    DocumentBuilderFactory dbFactoryLocal = DocumentBuilderFactory.newInstance();
                    //zahození kontrol na správnost XML
                    dbFactoryLocal.setValidating(false);
                    dbFactoryLocal.setNamespaceAware(false);
                    dbFactoryLocal.setIgnoringElementContentWhitespace(true);
                    //konec zahození kontrol
                    LOG.log(Level.INFO, " builderFactory");
                    try {
                        DocumentBuilder dBuilderLocal = dbFactoryLocal.newDocumentBuilder();
                        LOG.log(Level.INFO, " documentBuilder");
                        Document docLocal = null;
                        Boolean neniChyba = true;
                        try {
                            InputSource inputStream = new InputSource(new StringReader(xmlData));
                            docLocal = dBuilderLocal.parse(inputStream);
                        } catch (SAXException ex) { //potřebuji hlavně: SAXParseException
                            LOG.log(Level.SEVERE, " chyba SAX " + ex.getMessage());
                            neniChyba = false;
                        } catch (IOException ex) { 
                            LOG.log(Level.SEVERE, " chyba IO " + ex.getMessage());
                            neniChyba = false;
                        }
                        
                        if (neniChyba) {
                            NodeList nListIdentifiers = docLocal.getElementsByTagName("mods:modsCollection");
                            Node nNodeIdentifiers = nListIdentifiers.item(0);

                            krameriusEntry.setId(idPredlohaStr);
                            krameriusEntry.setLibraryId(rs2.getString("RDIGKNIHOVNA_DIGOBJEKT"));
                            krameriusEntry.setUuid(rs2.getString("UUID"));
                            krameriusEntry.setDruhDokumentu(rs2.getString("DRUHDOKUMENTU"));
                            krameriusEntry = ZpracujXML(nNodeIdentifiers, krameriusEntry);                        
                        }
                        
                        Connection connectionLocal = null;
                        PreparedStatement pstmt = null;
                        try {
                            connectionLocal = this.dataSource.getConnection();
                            pstmt = connectionLocal.prepareStatement("update digobjekt set json=? where id=" + idPredlohaStr);
                            pstmt.setString(1, Utils.toJson(krameriusEntry));
                            LOG.log(Level.INFO, " sql pro update digObjekt - json: " + ((oracle.jdbc.driver.OraclePreparedStatement) pstmt).getOriginalSql());
                            pstmt.execute();
                            connectionLocal.commit();
                        } catch (SQLException e) {
                            LOG.log(Level.INFO, " chyba při update digObjekt - json");
                        } finally {
                            Utils.tryClose(pstmt);
                            Utils.tryClose(connectionLocal);
                        }
                    } catch (SQLException e) {
                        LOG.log(Level.INFO, " chyba při spojování nespojených záznamů");
                    }

                }
                
                //LOG.log(Level.INFO, " json: " + Utils.toJson(krameriusEntry));
                if (krameriusEntry != null) {
                    if (zpracovavanyZaznam==0) {
                        zapisDoSouboru("bwSouborProNenalezeneZaznamyMonografie", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProNalezeneZaznamyMonografie", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProChybneZaznamyMonografie", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProNejednoznacneZaznamyMonografie", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProNenalezeneZaznamyPeriodika", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProNalezeneZaznamyPeriodika", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProChybneZaznamyPeriodika", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                        zapisDoSouboru("bwSouborProNejednoznacneZaznamyPeriodika", "\n\nZáznamy dodatečně spojované\n", "Záznamy dodatečně spojované");                
                    }
                    zpracovavanyZaznam++;
                    LOG.log(Level.INFO, " pokus o připojení předlohy");
                    pripojPredlohu(krameriusEntry);
                    LOG.log(Level.INFO, " konec pokusu o připojení předlohy");
                }
                LOG.log(Level.INFO, " záznam zpracován, další v pořadí následuje");
            }
        } catch (SQLException e) {
            LOG.log(Level.INFO, " chyba SQL při připojování záznamů Předloha->DigObjekt (najitPredlohyBezDigObjektu)");
        } catch (Exception e) {
            LOG.log(Level.INFO, " obecná chyba při připojování záznamů Předloha->DigObjekt (najitPredlohyBezDigObjektu)");
        } finally {
            Utils.tryClose(rs2);
            Utils.tryClose(rs);
            Utils.tryClose(stmt2);
            Utils.tryClose(stmt);
            if (zalozenaVlastniTransakce) { 
                try {
                    this.connection.commit(); 
                } catch (SQLException ex) {
                    LOG.log(Level.SEVERE, " nepodařil se commit u vlastní transakce");
                }
            }
        }

    }
    

}
