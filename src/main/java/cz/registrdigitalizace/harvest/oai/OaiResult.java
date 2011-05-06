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

import javax.xml.datatype.XMLGregorianCalendar;
import org.openarchives.oai2.OAIPMHtype;
import org.openarchives.oai2.RequestType;

/**
 *
 * @author Jan Pokorsky
 */
public abstract class OaiResult {

    private XMLGregorianCalendar responseDate;
    private RequestType request;

    protected OaiResult(OAIPMHtype oai) {
        this.responseDate = oai.getResponseDate();
        this.request = oai.getRequest();
    }

    public XMLGregorianCalendar getResponseDate() {
        return responseDate;
    }

    public RequestType getRequest() {
        return request;
    }

}
