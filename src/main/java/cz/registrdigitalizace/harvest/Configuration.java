/*
 * Copyright (C) 2017 Marek Kortus, based on 2012 jan Pokorsky
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.registrdigitalizace.harvest;

import static cz.registrdigitalizace.harvest.Harvest.CONFIG_PROPERTY;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Harvest configuration.
 */
final class Configuration {

    private static final String LIBRARY_IDS = "-libraryIDs";
    private static final String LIBRARY_ID = "-libraryID";
    private static final String FROM_DATE = "-fromDate";
    private static final String TO_DATE = "-toDate";
    private static final String HARVEST_DATA = "-harvestData";
    private static final String CREATE_THUMBNAILS = "-createThumbnails";
    private static final String SPOJIT_PREDLOHU_OBJEKT = "-spojitPredlohuObjekt";
    private static final String ZAPIS_DO_DATABAZE = "-zapisDoDatabaze";
    private static final String VERSION = "-version";
    private static final String HELP = "-help";
    private static final String H = "-h";
    private static final Logger LOG = Logger.getLogger(Configuration.class.getName());

    //private boolean dryRun;
    private boolean help;
    private boolean version;
    private List<String> errors = new ArrayList<String>();
    private Properties properties;
    
    private String libraryIDs = "";
    private String fromDate = "";
    private String toDate = "";
    private Boolean harvestData = true;
    private Boolean createThumbnails = false;
    private Boolean spojitPredlohuObjekt = true;
    
    private Boolean zapisDoSouboru = false;
    private Boolean zapisDoDatabaze = true;

    /** build configuration from command line */
    public static Configuration fromCmdLine(String[] args, Configuration conf) {
        //Configuration conf = new Configuration();
        try {
            parseCmdLine(conf, args);
        } catch (IllegalArgumentException ex) {
            conf.addError(ex.getLocalizedMessage());
        }
        return conf;
    }

    /**
     * parsuje parametry z příkazové řádky a podle nich nastaví proměnné
     * @param conf (Configuration)
     * @param args (String[])
     * @throws IllegalArgumentException 
     */
    static void parseCmdLine(Configuration conf, String[] args) throws IllegalArgumentException {
        String action = null;
        String pomocnyText;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ((arg.indexOf("=")>0) && (LIBRARY_IDS.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                if (conf.libraryIDs==null) {
                    conf.libraryIDs = arg.substring(arg.indexOf("=")+1);
                } else {
                    conf.libraryIDs += "," + arg.substring(arg.indexOf("=")+1);
                }
            } else if ((arg.indexOf("=")>0) && (LIBRARY_ID.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                if (conf.libraryIDs==null) {
                    conf.libraryIDs = arg.substring(arg.indexOf("=")+1);
                } else {
                    conf.libraryIDs += "," + arg.substring(arg.indexOf("=")+1);
                }
            } else if ((arg.indexOf("=")>0) && (FROM_DATE.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                conf.fromDate = arg.substring(arg.indexOf("=")+1);
            } else if ((arg.indexOf("=")>0) && (TO_DATE.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                conf.toDate = arg.substring(arg.indexOf("=")+1);
            } else if ((arg.indexOf("=")>0) && (HARVEST_DATA.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                pomocnyText = arg.substring(arg.indexOf("=")+1);
                if (("true".equals(pomocnyText)) || ("1".equals(pomocnyText))) {
                    conf.harvestData = true;
                } else {
                    conf.harvestData = false;
                }
                LOG.log(Level.INFO, " harvest nastaven na: " + conf.harvestData);
            } else if ((arg.indexOf("=")>0) && (CREATE_THUMBNAILS.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                pomocnyText = arg.substring(arg.indexOf("=")+1);
                if (("true".equals(pomocnyText)) || ("1".equals(pomocnyText))) {
                    conf.createThumbnails = true;
                } else {
                    conf.createThumbnails = false;
                }
            } else if ((arg.indexOf("=")>0) && (SPOJIT_PREDLOHU_OBJEKT.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                pomocnyText = arg.substring(arg.indexOf("=")+1);
                if (("true".equals(pomocnyText)) || ("1".equals(pomocnyText))) {
                    conf.spojitPredlohuObjekt = true;
                } else {
                    conf.spojitPredlohuObjekt = false;
                }
            } else if ((arg.indexOf("=")>0) && (ZAPIS_DO_DATABAZE.equalsIgnoreCase(arg.substring(0,arg.indexOf("="))))) {
                pomocnyText = arg.substring(arg.indexOf("=")+1);
                if (("true".equals(pomocnyText)) || ("1".equals(pomocnyText))) {
                    conf.zapisDoDatabaze = true;
                    conf.zapisDoSouboru = false;
                } else {
                    conf.zapisDoDatabaze = false;
                    conf.zapisDoSouboru = true;
                }
            } else if (HELP.equalsIgnoreCase(arg) || H.equals(arg)) {
                conf.help = true;
            } else {
                conf.addError(String.format("Unknown parameter '%s'", arg));
            }

        }
        if (conf.libraryIDs!=null) {
            conf.libraryIDs = Utils.deDuplikace(conf.libraryIDs);
        }
    }

    /**
     * nastaví id knihoven, které se mají zpracovat
     * @param idsArg (String)
     * @return (Set<BigDecimal>)
     */
    static Set<BigDecimal> parseIds(String idsArg) {
        String[] ids = idsArg.split("\\s*,\\s*");
        LinkedHashSet<BigDecimal> result = new LinkedHashSet<BigDecimal>(ids.length);
        for (String id : ids) {
            id = id.trim();
            result.add(new BigDecimal(id));
        }
        return result;
    }

    /**
     * přidá aktuální chybové hlášení do listu
     * @param msg (String)
     */
    private void addError(String msg) {
        errors.add(msg);
        help = true;
    }

    /**
     * vrátí help
     * @return (String)
     */
    public static String printHelp() {
        String tab = "  ";
        String nltab = "\n" + tab;
        String nltabtab = "\n" + tab + tab;
        return String.format("harvest [%s | %s | %s=<libIds> | %s=<libId> | %s=from_date | %s=to_date | %s=true/1/false/0]",
                        VERSION, HELP, LIBRARY_IDS, LIBRARY_ID, FROM_DATE, TO_DATE, HARVEST_DATA)
                + "\n\nLoad option from config file, but this can be overwrited by parameters. Definition of library connect string is loaded from database by id"
                + "\n\nOptions:"
                + nltab + VERSION
                + nltabtab + "Prints program version."
                + nltab + HELP + ", " + H
                + nltabtab + "Prints this help."
                + "\n\n";
    }

    /**
     * vrátí jestli se má vypsat verze
     * @return (boolean)
     */
    public boolean isVersion() {
        return version;
    }

    /**
     * vrátí jestli se má vypsat help
     * @return (boolean)
     */
    public boolean isHelp() {
        return help;
    }

    /**
     * vrátí list chyb
     * @return (List<String>)
     */
    public List<String> getErrors() {
        return errors;
    }

    /**
     * Gets harvester.properties.
     * @see Harvest#CONFIG_PROPERTY
     */
    public Properties getProperties() {
        return loadConfigFile();
    }

    /**
     * Fetches properties form file specified as
     * {@code -Dcz.registrdigitalizace.harvest.Harvest.config}.
     * @return (Properties)
     */
    public Properties loadConfigFile() {
        if (properties != null) {
            return properties;
        }
        properties = System.getProperties();
        String configPath = properties.getProperty(CONFIG_PROPERTY);
        LOG.log(Level.FINEST, "CONFIG_PROPERTY: {0}", configPath);
        if (configPath != null) {
            InputStreamReader reader = null;
            try {
                reader = new InputStreamReader(new FileInputStream(configPath), "UTF-8");
                properties = new Properties();
                properties.load(reader);
            } catch (IOException ex) {
                throw new IllegalStateException(configPath, ex);
            } finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                } catch (IOException ex) {
                    LOG.log(Level.SEVERE, configPath, ex);
                }
            }
        }
        LOG.log(Level.FINEST, "config: {0}", properties.toString());
        return properties;
    }

    /**
     * zjišťuje jestli option již nebyla nastavena
     * @param option (String)
     * @param arg (String)
     * @return (String)
     */
    private static String checkSingleOption(String option, String arg) {
        if (option != null) {
            throw new IllegalArgumentException("Multiple exclusive options! " + option + ", " + arg);
        }
        return arg;
    }

    //MK
    /**
     * Umožňuje nastavit jedno nebo více knihoven, které se mají načíst. Musí být odděleny znakem "," (čárka)
     * @param value (String)
     */
    public void SetLibraryIds(String value) {
        if (!Utils.jePrazdne(value)) { this.libraryIDs = value; }
    }

    /**
     * Umožňuje nastavit startovní datum harvesteru, pokud není nastaveno použije se datum poslední sklizně.
     * @param value (String)
     */
    public void SetFromDate(String value) {
        if (!Utils.jePrazdne(value)) { this.fromDate = value; }
    }

    /**
     * Umožňuje nastavit koncové datum harvesteru, pokud nenastaveno nastaví půlnoc dnešního dne ("01.01.2000T00:00:00Z"), čímž sklidí vše do ("31.12.1999T23:59:59Z")
     * @param value (String)
     */
    public void SetToDate(String value) {
        if (!Utils.jePrazdne(value)) { this.toDate = value; }
    }

    /**
     * vrací seznam knihoven
     * @return (String)
     */
    public String getLibraryIds() {
        return this.libraryIDs;
    }

    /**
     * vrací hodnotu fromDate
     * @return (String)
     */
    public String getFromDate() {
        return this.fromDate;
    }

    /**
     * vrací hodnotu toDate
     * @return (String)
     */
    public String getToDate() {
        return this.toDate;
    }

    /**
     * Umožňuje nastavit jestli se mají harvestrovat data (záznamy) knihovny
     * @param value (Boolean)
     */
    public void SetHarvestData(Boolean value) {
        if (!Utils.jePrazdne(value)) { this.harvestData = value; }
    }

    /**
     * vrací hodnotu zda se mají harvestrovat data (záznamy)
     * @return (Boolean)
     */
    public Boolean GetHarvestData() {
        return this.harvestData;
    }

    /**
     * vrací hodnotu zda se mají harvestrovat data (záznamy)
     * @return (Boolean)
     */
    public Boolean isHarvestData() {
        return this.harvestData;
    }

    /**
     * Umožňuje nastavit jestli se mají harvestrovat data (záznamy) knihovny
     * @param value (Boolean)
     */
    public void SetCreateThumbnails(Boolean value) {
        if (!Utils.jePrazdne(value)) { this.createThumbnails = value; }
    }

    /**
     * vrací hodnotu zda se mají harvestrovat data (záznamy)
     * @return (Boolean)
     */
    public Boolean GetCreateThumbnails() {
        return this.createThumbnails;
    }

    /**
     * vrací hodnotu zda se mají vytvářet thumbnails (náhledy)
     * @return (Boolean)
     */
    public Boolean isCreateThumbnails() {
        return this.createThumbnails;
    }

    /**
     * vrací hodnotu zda se má zapisovat do databáze
     * @return (Boolean)
     */
    public Boolean isZapisDoDatabaze() {
        return this.zapisDoDatabaze;
    }

    /**
     * vrací hodnotu zda se má zapisovat do souboru
     * @return (Boolean)
     */
    public Boolean isZapisDoSouboru() {
        return this.zapisDoSouboru;
    }

    /**
     * vrací hodnotu zda se má propojit Predloha a DigObjekt
     * @return (Boolean)
     */
    public Boolean isSpojitPredlohuObjekt() {
        return this.spojitPredlohuObjekt;
    }

    //MK konec

    /**
     * vrací kompletní data
     * @return (String)
     */
    public String GetFullData() {
        String data = "";
        //data += "dryRun: " + dryRun;
        data += ", help: " + help;
        data += ", version: " + version;
        data += ", errors: (" + errors.size() + ")";
        data += ", libraryIDs: " + libraryIDs;
        data += ", fromDate: " + fromDate;
        data += ", toDate: " + toDate;
        data += ", harvestData: " + harvestData;

        return data;
    }
    
    /**
     * plní properties z konfiguračního souboru
     */
    public void fillPropertiesFromConfigFile() {
        Properties propertie = loadConfigFile();
        String pomocnyString = "";

        Enumeration e = properties.propertyNames();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            if (("libraryID".equalsIgnoreCase(key)) || ("libraryIDs".equalsIgnoreCase(key))) {
                libraryIDs = properties.getProperty(key).trim();
            } else if ("fromDate".equalsIgnoreCase(key)) {
                fromDate = properties.getProperty(key).trim();
            } else if ("toDate".equalsIgnoreCase(key)) {
                toDate = properties.getProperty(key).trim();
            } else if ("harvestData".equalsIgnoreCase(key)) {
                pomocnyString = properties.getProperty(key).trim();
                if (("true".equals(pomocnyString)) || ("1".equals(pomocnyString))) {
                    harvestData = true;
                } else {
                    harvestData = false;                    
                }
            } else if ("zapisDoDatabaze".equalsIgnoreCase(key)) {
                pomocnyString = properties.getProperty(key).trim();
                if (("true".equals(pomocnyString)) || ("1".equals(pomocnyString))) {
                    zapisDoSouboru = false;
                    zapisDoDatabaze = true;
                } else {
                    zapisDoSouboru = true;
                    zapisDoDatabaze = false;
                }
            } else if ("spojitPredlohuObjekt".equalsIgnoreCase(key)) {
                pomocnyString = properties.getProperty(key).trim();
                if (("true".equals(pomocnyString)) || ("1".equals(pomocnyString))) {
                    spojitPredlohuObjekt = true;
                } else {
                    spojitPredlohuObjekt = false;
                }
            }
        }
    }

}
