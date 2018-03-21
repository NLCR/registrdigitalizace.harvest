/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.registrdigitalizace.harvest.db;

import cz.registrdigitalizace.harvest.Utils;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mkortus
 */
public class NazevEntry {
    private String type;
    private String title;
    private String subTitle;
    private String partNumber;
    private String partName;
    private String displayLabel;
    private Boolean alternative;
    private List<String> nezpracovanaHodnota;
    private Boolean empty;
    private String fullTitle;
    
    public NazevEntry() {
        this.type = "";
        this.title = "";
        this.subTitle = "";
        this.partNumber = "";
        this.partName = "";
        this.displayLabel = "";
        this.alternative = false;
        this.nezpracovanaHodnota = new ArrayList<String>();
        this.empty = false;
        this.fullTitle = "";
    }
        
    public String getType() {
        return this.type;
    }
    
    public void setType(String valueLocal) {
        if ((valueLocal != null) && (!valueLocal.isEmpty())) {
            this.type = valueLocal;
        }
    }

    public String getFullTitle() {
        String vystup = "";
        if (!Utils.jePrazdne(this.title)) vystup += this.title;
        //if (!Utils.jePrazdne(this.subTitle)) vystup += ": " + this.subTitle;
        //if (!Utils.jePrazdne(this.partNumber)) vystup += ". " + this.partNumber;
        //if (!Utils.jePrazdne(this.partName)) vystup += ". " + this.partName;
        return vystup;
    }
    
    public String getTitle() {
        return this.title;
    }
    
    public void setTitle(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.title = valueLocal;
        }
    }

    public String getSubTitle() {
        return this.subTitle;
    }
    
    public void setSubTitle(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.subTitle = valueLocal;
        }
    }

    public String getPartNumber() {
        return this.partNumber;
    }
    
    public void setPartNumber(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.partNumber = valueLocal;
        }
    }

    public String getPartName() {
        return this.partName;
    }
    
    public void setPartName(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.partName = valueLocal;
        }
    }

    public String getDisplayLabel() {
        return this.displayLabel;
    }
    
    public void setDisplayLabel(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.displayLabel = valueLocal;
        }
    }

    public Boolean getAlternative() {
        return this.alternative;
    }
    
    public void setAlternative(Boolean valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.alternative = valueLocal;
        }
    }

    public List<String> getNezpracovaneHodnoty() {
        return this.nezpracovanaHodnota;
    }
    
    public void setNezpracovaneHodnoty(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nezpracovanaHodnota.addAll(valueLocal);
        }
    }

    public void addNezpracovaneHodnoty(String value) {
        this.nezpracovanaHodnota.add(value);
    }

    public void addNezpracovaneHodnotyAll(List<String> value) {
        this.nezpracovanaHodnota.addAll(value);
    }

    public String toString() {
        String vystup = "";
        if (!Utils.jePrazdne(getType())) vystup += "type: " + getType() + "\n";
        if (!Utils.jePrazdne(getTitle())) vystup += "title: " + getFullTitle() + "\n";
        if (!Utils.jePrazdne(getDisplayLabel())) vystup += "displayLabel: " + getDisplayLabel() + "\n";
        if (!Utils.jePrazdne(getAlternative())) vystup += "alternative: " + getAlternative() + "\n";

                
        if (!Utils.jePrazdne(getNezpracovaneHodnoty())) vystup += "\n  nezpracované hodnoty: " + listToString(getNezpracovaneHodnoty());
        return vystup;
    }
    
    private String listToString(List<String> hodnoty) {
        String vystup = "";
        if ((hodnoty!=null) && (hodnoty.size()>0)) {
            for (int i=0; i<hodnoty.size(); i++) {
                if (!"".equals(hodnoty.get(i))) {
                    if (!"".equals(vystup)) vystup += ", ";
                    vystup += hodnoty.get(i);
                }
            }
        }
        return vystup;
    }

    public Boolean isEmpty() {
        Boolean vysledek = false;
        if (Utils.jePrazdne(this.title)) {
            return true;
        }
        return vysledek;
    }

    public String hodnota() {
        return Utils.normalize(getFullTitle());
    }

}
