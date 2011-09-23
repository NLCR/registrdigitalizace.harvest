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

import cz.registrdigitalizace.harvest.metadata.DigobjectType;
import java.math.BigDecimal;
import java.util.List;

/**
 * DIGMETADATA table descriptor.
 *
 * @author Jan Pokorsky
 */
public class Metadata {
    
    /** DigObject reference */
    private BigDecimal id;
    private String title;
    private String authors;
    private String publishers;
    private String issn;
    private String isbn;
    private String ccnb;
    private String sigla;
    private String signature;
    private String yearOfPublication;

    public BigDecimal getId() {
        return id;
    }

    public void setId(BigDecimal id) {
        this.id = id;
    }

    public String getAuthors() {
        return authors;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public String getCcnb() {
        return ccnb;
    }

    public void setCcnb(String ccnb) {
        this.ccnb = ccnb;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public String getIssn() {
        return issn;
    }

    public void setIssn(String issn) {
        this.issn = issn;
    }

    public String getPublishers() {
        return publishers;
    }
    
    public void setPublishers(String publishers) {
        this.publishers = publishers;
    }

    public String getSigla() {
        return sigla;
    }

    public void setSigla(String sigla) {
        this.sigla = sigla;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getYearOfPublication() {
        return yearOfPublication;
    }

    public void setYearOfPublication(String yearOfPublication) {
        this.yearOfPublication = yearOfPublication;
    }

    public static Metadata create(DigobjectType dt, BigDecimal digObjectId) {
        Metadata m = new Metadata();
        m.setId(digObjectId);
        if (dt != null) {
            m.setAuthors(toString(dt.getAuthor(), ", "));
            m.setCcnb(dt.getCcnb());
            m.setIsbn(dt.getIsbn());
            m.setIssn(dt.getIssn());
            m.setPublishers(toString(dt.getPublisher(), ", "));
            m.setSigla(dt.getSigla());
            m.setSignature(toString(dt.getSignature(), "|"));
            m.setTitle(dt.getTitle());
            m.setYearOfPublication(toString(dt.getYear(), "|"));
        }
        return m;
    }

    private static String toString(List<String> l, String separator) {
        assert separator != null;

        String result;
        if (l == null || l.isEmpty()) {
            result = null;
        } else if (l.size() == 1) {
            result = l.get(0);
        } else {
            StringBuilder sb = new StringBuilder();
            for (String item : l) {
                if (item != null && item.length() > 0) {
                    sb.append(separator).append(item);
                }
            }
            result = sb.length() > 0 ? sb.substring(separator.length()): null;
        }
        return result;
    }
    
}
