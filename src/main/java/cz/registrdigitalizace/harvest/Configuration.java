/*
 * Copyright (C) 2012 Jan Pokorsky
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

import java.util.ArrayList;
import java.util.List;

/**
 * Harvest configuration.
 */
final class Configuration {

    private static final String UPDATE_METADATA = "-updateMetadata";
    private static final String UPDATE_THUMBNAILS = "-updateThumbnails";
    private static final String VERSION = "-version";
    private static final String HARVEST_TO_CACHE = "-harvestToCache";
    private static final String HARVEST_FROM_CACHE = "-harvestFromCache";
    private static final String HARVEST_WITH_CACHE = "-harvestWithCache";
    private static final String CACHE_ROOT = "-cacheRoot";
    private static final String DRY_RUN = "-dryRun";
    private static final String HELP = "-help";
    private static final String H = "-h";
    
    private boolean dryRun;
    private boolean harvestFromCache;
    private boolean harvestToCache;
    private boolean harvestWithCache;
    private boolean help;
    private boolean regenerateMods;
    private boolean updateThumbnails;
    private boolean version;
    private String cachePath;
    private String cacheRoot;
    private List<String> errors = new ArrayList<String>();

    /** build configuration from command line */
    public static Configuration fromCmdLine(String[] args) {
        Configuration conf = new Configuration();
        try {
            parseCmdLine(conf, args);
        } catch (IllegalArgumentException ex) {
            conf.addError(ex.getLocalizedMessage());
        }
        return conf;
    }

    static void parseCmdLine(Configuration conf, String[] args) throws IllegalArgumentException {
        String action = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (UPDATE_METADATA.equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.regenerateMods = true;
            } else if (UPDATE_THUMBNAILS.equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.updateThumbnails = true;
            } else if (VERSION.equals(arg)) {
                conf.version = true;
            } else if (HARVEST_TO_CACHE.equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvestToCache = true;
            } else if (HARVEST_FROM_CACHE.equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvestFromCache = true;
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing session cache folder!");
                }
                conf.cachePath = args[++i];
            } else if (HARVEST_WITH_CACHE.equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvestWithCache = true;
            } else if (CACHE_ROOT.equals(arg)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing cache root folder!");
                }
                conf.cacheRoot = args[++i];
            } else if (DRY_RUN.equals(arg)) {
                conf.dryRun = true;
            } else if (HELP.equals(arg) || H.equals(arg)) {
                conf.help = true;
            } else {
                conf.addError(String.format("Unknown parameter '%s'", arg));
            }
        }
    }

    private void addError(String msg) {
        errors.add(msg);
        help = true;
    }

    public static String help() {
        String tab = "  ";
        String nltab = "\n" + tab;
        String nltabtab = "\n" + tab + tab;
        return String.format("harvest [%s | %s | %s | %s | %s]", VERSION, HELP, UPDATE_METADATA, UPDATE_THUMBNAILS, DRY_RUN)
                + String.format("\nharvest %s [%s <folder>] | %s [%s <folder>]",
                        HARVEST_TO_CACHE, CACHE_ROOT, HARVEST_WITH_CACHE, CACHE_ROOT)
                + "\nharvest -harvestFromCache <folder>"
                + "\n\nWithout options it harvests data from remote OAI sources (DIGKNIHOVNA table) and writes them to DB."
                + "\n\nOptions:"
                + nltab + UPDATE_METADATA
                + nltabtab + "Recomputes meta data from already harvested XML inside DB. No harvest"
                + nltab + UPDATE_THUMBNAILS
                + nltabtab + "Removes thumbnail of deleted digital objects and fetches missing. No harvest"
                + nltab + VERSION
                + nltabtab + "Prints program version."
                + nltab + HELP + ", " + H
                + nltabtab + "Prints this help."
                + nltab + HARVEST_TO_CACHE
                + nltabtab + "Harvests data to local cache. None data are written to DB."
                + nltab + HARVEST_WITH_CACHE
                + nltabtab + "Harvests data to local cache and then writes it to DB."
                + nltab + String.format("%s <folder>", HARVEST_FROM_CACHE)
                + nltabtab + "Reads already harvested data from foilder containing session cache and writes it to DB."
                + nltab + String.format("%s <folder>", CACHE_ROOT)
                + nltabtab + "Path to store  all harvested data. -Djava.io.tmp/harvest_cache is default path."
                + nltab + DRY_RUN
                + nltabtab + "Use to test storage of newly harvested records. Rollbacks DB modifications."
                + "\n\n";
    }

    public boolean isRegenerateMods() {
        return regenerateMods;
    }

    public boolean isVersion() {
        return version;
    }

    public boolean isHarvestFromCache() {
        return harvestFromCache;
    }

    public boolean isHarvestToCache() {
        return harvestToCache;
    }

    public boolean isHarvestWithCache() {
        return harvestWithCache;
    }

    public boolean isHelp() {
        return help;
    }

    public String getCachePath() {
        return cachePath;
    }

    public String getCacheRoot() {
        if (cacheRoot == null) {
            cacheRoot = defaultCacheRoot();
        }
        return cacheRoot;
    }

    public boolean isUpdateThumbnails() {
        return updateThumbnails;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public List<String> getErrors() {
        return errors;
    }

    static String defaultCacheRoot() {
        return System.getProperty("java.io.tmpdir") + "/harvest_cache";
    }

    private static String checkSingleOption(String option, String arg) {
        if (option != null) {
            throw new IllegalArgumentException("Multiple exclusive options! " + option + ", " + arg);
        }
        return arg;
    }

}
