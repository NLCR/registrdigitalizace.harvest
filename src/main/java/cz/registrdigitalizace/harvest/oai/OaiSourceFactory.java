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

import java.net.MalformedURLException;

/**
 *
 * @author Jan Pokorsky
 */
public class OaiSourceFactory {
    
    public static final String FACTORY_PROP = "cz.registrdigitalizace.harvest.oai.OaiSourceFactory";

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

    public OaiSource createListRecords(String baseUriStr, String fromDate,
            String metadataPrefix, String otherParams) throws OaiException {
        try {
            return OaiSource.createListRecords(baseUriStr, fromDate, metadataPrefix, otherParams);
        } catch (MalformedURLException ex) {
            throw new OaiException(ex);
        }
    }

}
