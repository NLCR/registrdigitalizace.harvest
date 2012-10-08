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

import java.io.File;
import java.math.BigDecimal;

/**
 * The class describes DIGKNIHOVNA table record.
 *
 * @author Jan Pokorsky
 */
public final class Library {

    private BigDecimal id;
    /** PROTOKOL */
    private String harvestProtocol;
    /** FORMATDAT */
    private String metadataFormat;
    /** LASTHARVEST */
    private String lastHarvest;
    /** OAIPMHSERVERBASEURL */
    private String baseUrl;
    /** OAIPMHCOMMAND */
    private String queryParameters;
    private transient File cacheFolder;

    /** checks library fields and returns error message in case of any illegal field */
    public String validate() {
        StringBuilder err = new StringBuilder();
        if (id == null) {
            err.append("\n Missing ID.");
        }
        if (harvestProtocol == null) {
            err.append("\n Missing PROTOKOL.");
        }
        if (baseUrl == null) {
            err.append("\n Missing OAIPMHSERVERBASEURL.");
        }
        return err.length() == 0 ? null : err.toString();
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String getHarvestProtocol() {
        return harvestProtocol;
    }

    public void setHarvestProtocol(String harvestProtocol) {
        this.harvestProtocol = harvestProtocol;
    }

    public BigDecimal getId() {
        return id;
    }

    public void setId(BigDecimal id) {
        this.id = id;
    }

    public String getLastHarvest() {
        return lastHarvest;
    }

    public void setLastHarvest(String lastHarvest) {
        this.lastHarvest = lastHarvest;
    }

    public String getMetadataFormat() {
        return metadataFormat;
    }

    public void setMetadataFormat(String metadataFormat) {
        this.metadataFormat = metadataFormat;
    }

    public String getQueryParameters() {
        return queryParameters;
    }

    public void setQueryParameters(String queryParameters) {
        this.queryParameters = queryParameters;
    }

    public File getCacheFolder() {
        return cacheFolder;
    }

    public void setCacheFolder(File cacheFolder) {
        this.cacheFolder = cacheFolder;
    }

    @Override
    public String toString() {
        return String.format("Library[id: %s, baseUrl: %s, protocol: %s, format: %s, query: %s, last: %s]",
                id, baseUrl, harvestProtocol, metadataFormat, queryParameters, lastHarvest);
    }

}
