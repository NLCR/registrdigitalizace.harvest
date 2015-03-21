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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlValue;

/**
 * METADATA table descriptor.
 *
 * @author Jan Pokorsky
 */
@XmlRootElement(name = "metadata")
@XmlAccessorType(XmlAccessType.FIELD)
public class Metadata {

    // relief names
    public static final String CISLO = "cislo";
    public static final String CCNB = "ccnb";
    public static final String ISBN = "isbn";
    public static final String ISSN = "issn";
    public static final String KATALOG = "katalog";
    public static final String NAZEV = "nazev";
    public static final String OSOBA = "osoba";
    public static final String POLE001 = "pole001";
    public static final String ROK_VYDANI = "rokVydani";
    public static final String SIGLA_BIB_UDAJU = "siglaBibUdaju";
    public static final String SIGLA_FYZ_JEDNOTKY = "siglaFyzJednotky";
    public static final String SIGNATURA = "signatura";
    public static final String URNNBN = "urnnbn";

    @XmlTransient
    private BigDecimal digObjId;
    @XmlElement(name = "item")
    private List<MetadataItem> items;

    public BigDecimal getDigObjId() {
        return digObjId;
    }

    public void setDigObjId(BigDecimal digObjId) {
        this.digObjId = digObjId;
    }

    public List<MetadataItem> getItems() {
        if (items == null) {
            items = new ArrayList<MetadataItem>();
        }
        return items;
    }

    /**
     * Finds {@link MetadataItem} values for a given name.
     * @param name relief name to search
     * @return list of values
     */
    public List<String> find(String name) {
        return find(name, null);
    }

    /**
     * Finds {@link MetadataItem} values for a given name.
     * @param name relief name to search
     * @param invalid filter invalid attribute; {@code null} stands for all
     * @return list of values
     */
    public List<String> find(String name, Boolean invalid) {
        return find(getItems(), name, invalid);
    }

    public static List<String> find(List<MetadataItem> items, String name) {
        return find(items, name, null);
    }

    public static List<String> find(List<MetadataItem> items, String name, Boolean invalid) {
        ArrayList<String> values = new ArrayList<String>();
        for (MetadataItem item : items) {
            boolean include = name == null || name.equals(item.getReliefName());
            include &= invalid == null || invalid.equals(item.isInvalid());
            if (include) {
                values.add(item.getValue());
            }
        }
        return values;
    }

    @Override
    public String toString() {
        return "Metadata2{digObjId=" + digObjId + ", items=" + items + '}';
    }
    
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class MetadataItem {

        @XmlTransient
        private BigDecimal id;
        @XmlValue
        private String value;
        @XmlAttribute
        private String reliefName;
        @XmlAttribute
        private Boolean invalid;

        public MetadataItem() {
        }

        public MetadataItem(BigDecimal id, String value, String reliefName, Boolean invalid) {
            this.id = id;
            this.value = value;
            this.reliefName = reliefName;
            this.invalid = invalid;
        }

        public BigDecimal getId() {
            return id;
        }

        public void setId(BigDecimal id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getReliefName() {
            return reliefName;
        }

        public void setReliefName(String reliefName) {
            this.reliefName = reliefName;
        }

        public boolean isInvalid() {
            return invalid != null && invalid;
        }

        public Boolean getInvalid() {
            return invalid;
        }

        public void setInvalid(Boolean invalid) {
            this.invalid = invalid;
        }

        @Override
        public String toString() {
            return "MetadataItem{" + "id=" + id + ", value=" + value
                    + ", reliefName=" + reliefName + ", invalid=" + invalid + '}';
        }

    }
}
