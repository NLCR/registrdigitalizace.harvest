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

package cz.registrdigitalizace.harvest.oai;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory building various OAI sources.
 *
 * @author Jan Pokorsky
 */
public class OaiSourceFactory {

    /**
     * System property to provide custom factory class.
     */
    public static final String FACTORY_PROP = "cz.registrdigitalizace.harvest.oai.OaiSourceFactory";
    private static final Logger LOG = Logger.getLogger(OaiSourceFactory.class.getName());

    public static OaiSourceFactory getInstance() {
        String className = System.getProperty(FACTORY_PROP, FACTORY_PROP);
        try {
            Class clazz = Class.forName(className);
            return (OaiSourceFactory) clazz.newInstance();
        } catch (InstantiationException ex) {
            throw new IllegalStateException(className, ex);
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(className, ex);
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(className, ex);
        }
    }

//    public OaiSource createListRecords(String baseUriStr, String fromDate,
//            String metadataPrefix, String otherParams) throws OaiException {
//        return createListRecords(baseUriStr, fromDate, null, metadataPrefix, otherParams);
/*
        try {
            return createListRecordsImpl(baseUriStr, fromDate, metadataPrefix, otherParams);
        } catch (URISyntaxException ex) {
            throw new OaiException(ex);
        } catch (MalformedURLException ex) {
            throw new OaiException(ex);
        }
*/
//    }

    /**
     * Marek přidán parametr toDate
     * @param baseUriStr
     * @param fromDate
     * @param toDate
     * @param metadataPrefix
     * @param otherParams
     * @return
     * @throws OaiException 
     */
    public OaiSource createListRecords(String baseUriStr, String verbParameterStr, String lastHarvest, String fromDate, String toDate,
            String metadataPrefix, String otherParams, String resumptionToken) throws OaiException {
        LOG.log(Level.INFO, " vytváří se list záznamů");
        try {
            return createListRecordsImpl(baseUriStr, verbParameterStr, lastHarvest, fromDate, toDate, metadataPrefix, otherParams, resumptionToken);
        } catch (URISyntaxException ex) {
            throw new OaiException(ex);
        } catch (MalformedURLException ex) {
            throw new OaiException(ex);
        }
    }
    
    /**
     * Creates OAI source to read cached responses from file system.
     * @param cache cache folder
     * @return OAI source
     * @see #createCachedSource
     */
    public OaiSource createSourceFromCache(File cache) {
        return new OaiLocalReadSource(cache);
    }

    /**
     * Creates OAI source that dumps responses to file system. It is necessary
     * to iterate all records fetched with e.g. {@link Harvester#getListRecords}
     * to fill the cache.
     * @param src remote OAI source to fetch responses
     * @param cache folder to store responses. Folder must exist an be empty.
     * @return OAI source
     * @see #createSourceFromCache
     */
    public OaiSource createCachedSource(OaiSource src, File cache) {
        return new OaiLocalWriteSource(src, cache);
    }

    /**
     * Marek přidán parametr toDate, resumptionToken
     * @param baseUriStr
     * @param fromDate
     * @param toDate
     * @param metadataPrefix
     * @param otherParams
     * @return
     * @throws URISyntaxException
     * @throws MalformedURLException 
     */
    static OaiSource createListRecordsImpl(String baseUriStr, String verbParameterStr, String lastHarvest, String fromDate, String toDate,
            String metadataPrefix, String otherParams, String resumptionToken) throws URISyntaxException, MalformedURLException {

        URI baseUri = new URI(baseUriStr);
        String query = baseUri.getQuery();
        if (query != null) {
            LOG.log(Level.WARNING, "Ignoring query part: {0}", baseUriStr);
        }
        String verbParameter = "verb=ListRecords";
        if ((verbParameterStr!=null) && (!"".equals(verbParameterStr))) {
            verbParameter = "verb=" + verbParameterStr;
        }
        StringBuilder oaiQuery = new StringBuilder(verbParameter);
        if ((resumptionToken!=null) && (!"".equals(resumptionToken))) {
            oaiQuery.append("&resumptionToken=").append(resumptionToken);
        } else {
            if (((fromDate == null) || (fromDate.length() == 0)) && (lastHarvest != null)) {
                fromDate = lastHarvest;
            }
            if (fromDate != null && fromDate.length() > 0) {
                oaiQuery.append("&from=").append(fromDate);
            }
            if (toDate != null && toDate.length() > 0) {
                oaiQuery.append("&until=").append(toDate);
            }
            if (metadataPrefix != null && metadataPrefix.length() > 0) {
                oaiQuery.append("&metadataPrefix=").append(metadataPrefix);
            }
        }
        if (otherParams != null && otherParams.length() > 0) {
            if (otherParams.charAt(0) != '&') {
                oaiQuery.append('&');
            }
            oaiQuery.append(otherParams);
        }

        baseUri = new URI(baseUri.getScheme(), baseUri.getUserInfo(),
                baseUri.getHost(), baseUri.getPort(), baseUri.getPath(),
                oaiQuery.toString(), baseUri.getFragment());
        baseUri.toURL(); // make sure getUrl will pass
        return new OaiSource(baseUri, verbParameter);
    }

}
