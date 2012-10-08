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

/**
 * Harvest configuration.
 */
final class Configuration {
    private boolean harvesFromCache;
    private boolean harvestToCache;
    private boolean harvestWithCache;
    private boolean help;
    private boolean regenerateMods;
    private boolean version;
    private String cachePath;
    private String cacheRoot;

    /** build configuration from command line */
    public static Configuration fromCmdLine(String[] args) {
        Configuration conf = new Configuration();
        String action = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("-updateMetadata".equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.regenerateMods = true;
            } else if ("-version".equals(arg)) {
                conf.version = true;
            } else if ("-harvestToCache".equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvestToCache = true;
            } else if ("-harvestFromCache".equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvesFromCache = true;
            } else if ("-harvestWithCache".equals(arg)) {
                action = checkSingleOption(action, arg);
                conf.harvestWithCache = true;
            } else if ("-cachePath".equals(arg) && i < args.length + 1) {
                conf.cachePath = args[++i];
            } else if ("-cacheRoot".equals(arg) && i < args.length + 1) {
                conf.cacheRoot = args[++i];
            } else if ("-help".equals(arg) || "-h".equals(arg)) {
                conf.help = true;
            }
        }
        if (conf.isHarvesFromCache() && conf.cachePath == null) {
            throw new IllegalStateException("Missing cache path!");
        }
        return conf;
    }

    public static String help() {
        String tab = "  ";
        String nltab = "\n" + tab;
        String nltabtab = "\n" + tab + tab;
        return "harvest [-version | -h | -updateMetadata]"
                + "\nharvest -harvestToCache [-cacheRoot <folder>] | -harvestWithCache [-cacheRoot <folder>]"
                + "\nharvest -harvestFromCache -cachePath <folder>"
                + "\n\nWithout options it harvests data from remote OAI sources (DIGKNIHOVNA table) and writes them to DB."
                + "\n\nOptions:"
                + nltab + "-updateMetadata"
                + nltabtab + "Recomputes meta data from already harvested XML inside DB. No harvest"
                + nltab + "-version"
                + nltabtab + "Prints program version."
                + nltab + "-help, -h"
                + nltabtab + "Prints this help."
                + nltab + "-harvestToCache"
                + nltabtab + "Harvests data to local cache. None data are written to DB."
                + nltab + "-harvestWithCache"
                + nltabtab + "Harvests data to local cache and then writes it to DB."
                + nltab + "-harvestFromCache"
                + nltabtab + "Reads already harvested data from -cachePath and writes it to DB."
                + nltab + "-cachePath <folder>"
                + nltabtab + "Path to harvested data."
                + nltab + "-cacheRoot <folder>"
                + nltabtab + "Path to store  all harvested data. -Djava.io.tmp/harvest_cache is default path."
                + "\n\n";
    }

    public boolean isRegenerateMods() {
        return regenerateMods;
    }

    public boolean isVersion() {
        return version;
    }

    public boolean isHarvesFromCache() {
        return harvesFromCache;
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
            cacheRoot = System.getProperty("java.io.tmpdir") + "/harvest_cache";
        }
        return cacheRoot;
    }

    private static String checkSingleOption(String option, String arg) {
        if (option != null) {
            throw new IllegalArgumentException("Multiple exclusive options! " + option + ", " + arg);
        }
        return arg;
    }

}
