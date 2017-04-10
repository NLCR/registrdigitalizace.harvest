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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * The class describes DIGKNIHOVNA table record.
 *
 * @author Jan Pokorsky
 */
public final class Library {

    private BigDecimal id;
    /** DLISTS.VALUE*/
    private String dListValue;
    /** PROTOKOL */
    private String harvestProtocol;
    /** FORMATDAT */
    private String metadataFormat;
    /** LASTHARVEST */
    private String lastHarvest;
    /** od jakého datumu */
    private String fromDate;
    /** do jakého datumu */
    private String toDate;
    /** OAIPMHSERVERBASEURL */
    private String baseUrl;
    /** OAIPMHCOMMAND */
    private String queryParameters;
    private transient File cacheFolder;
    
    private Integer pocetMesicu = 3;

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

    public String getDListValue() {
        return dListValue;
    }

    public void setDListValue(String dListValue) {
        this.dListValue = dListValue;
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

    public String getFromDate() {
        return fromDate;
    }

    public void setFromDate( String fromDate) {
        this.fromDate = fromDate;
    }

    public String getToDate() {
        return toDate;
    }

    public void setToDate(String toDate) {
        this.toDate = toDate;
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
        return String.format("Library[id: %s, dListValue: %s, baseUrl: %s, protocol: %s, format: %s, query: %s, last: %s, from: %s, to: %s]",
                id, dListValue, baseUrl, harvestProtocol, metadataFormat, queryParameters, lastHarvest, fromDate, toDate);
    }

}
