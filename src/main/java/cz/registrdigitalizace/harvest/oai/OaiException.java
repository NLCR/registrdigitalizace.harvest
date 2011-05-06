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

import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.openarchives.oai2.OAIPMHerrorType;

/**
 *
 * @author Jan Pokorsky
 */
public class OaiException extends Exception {

    private final List<OAIPMHerrorType> errors;
    private URL source;

    public OaiException(List<OAIPMHerrorType> errors) {
        this(errors, null);
    }
    
    public OaiException(List<OAIPMHerrorType> errors, URL source) {
        this(null, errors, source);
    }

    public OaiException(Throwable cause) {
        this(cause, null, null);
    }

    public OaiException(Throwable cause, URL source) {
        this(cause, null, source);
    }

    private OaiException(Throwable cause, List<OAIPMHerrorType> errors, URL source) {
        super(cause);
        this.errors = errors != null
                ? errors : Collections.<OAIPMHerrorType>emptyList();
        this.source = source;
    }


    public List<OAIPMHerrorType> getErrors() {
        return errors;
    }

    public URL getSource() {
        return source;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        String message = super.getMessage();
        if (message != null) {
            sb.append(message);
        }
        if (source != null) {
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append("url: ").append(source.toExternalForm());
        }
        for (OAIPMHerrorType error : errors) {
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append("error: ").append(error.getCode())
                    .append(", ").append(error.getValue());
        }
        return sb.toString();
    }

    void setSource(URL source) {
        this.source = source;
    }

}
