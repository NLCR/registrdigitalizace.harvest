/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.registrdigitalizace.harvest.db;

import cz.registrdigitalizace.harvest.Utils;
import java.util.List;

/**
 *
 * @author mkortus
 */
public class NakladatelEntry {
    private String nakladatel;
    private String datumVydani;
    private String poradiVydani;
    private String edice;
    private String vydani;
    private String mistoVydani;
    private String neidentifikovanaPole;
    
    public NakladatelEntry() {
    this.nakladatel = "";
    this.datumVydani = "";
    this.poradiVydani = "";
    this.edice = "";
    this.vydani = "";
    this.mistoVydani = "";
    this.neidentifikovanaPole = "";
    }
    
    public String getNakladatel() {
        return this.nakladatel;
    }
    
    public void setNakladatel(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nakladatel = valueLocal;
        }
    }

    public String getDatumVydani() {
        return this.datumVydani;
    }
    
    public void setDatumVydani(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.datumVydani = valueLocal;
        }
    }

    public String getPoradiVydani() {
        return this.poradiVydani;
    }
    
    public void setPoradiVydani(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.poradiVydani = valueLocal;
        }
    }

    public String getEdice() {
        return this.edice;
    }
    
    public void setEdice(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.edice = valueLocal;
        }
    }

    public String getVydani() {
        return this.vydani;
    }
    
    public void setVydani(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.vydani = valueLocal;
        }
    }

    public String getMistoVydani() {
        return this.mistoVydani;
    }
    
    public void setMistoVydani(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.mistoVydani = valueLocal;
        }
    }

    public String getNeindefikovanaPole() {
        return this.neidentifikovanaPole;
    }
    
    public void setNeindefikovanaPole(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.neidentifikovanaPole = valueLocal;
        }
    }


    public String toString() {
        String vystup = "";
        if (!Utils.jePrazdne(getNakladatel())) vystup += "nakladatel: " + getNakladatel();
        if (!Utils.jePrazdne(getDatumVydani())) vystup += "\n  datumVydani: " + getDatumVydani();
        if (!Utils.jePrazdne(getPoradiVydani())) vystup += "\n  poradiVydani: " + getPoradiVydani();
        if (!Utils.jePrazdne(getEdice())) vystup += "\n  edice: " + getEdice();
        if (!Utils.jePrazdne(getVydani())) vystup += "\n  vydani: " + getVydani();
        if (!Utils.jePrazdne(getMistoVydani())) vystup += "\n  mistoVydani: " + getMistoVydani();

        
        if (!Utils.jePrazdne(getNeindefikovanaPole())) vystup += "\n  neidentifikovaná pole: " + getNeindefikovanaPole();
        return vystup;
    }

    public Boolean isEmpty() {
        Boolean vysledek = false;
        if (
                (Utils.jePrazdne(this.nakladatel)) & (Utils.jePrazdne(this.datumVydani)) &
                (Utils.jePrazdne(this.poradiVydani)) & (Utils.jePrazdne(this.vydani)) & (Utils.jePrazdne(this.mistoVydani))
           ) {
            vysledek = true;
            return true;
        }
        return vysledek;
    }

    public String hodnota() {
        String vystup = "";
        if (!Utils.jePrazdne(this.getNakladatel())) vystup += this.getNakladatel();
        if (!Utils.jePrazdne(this.getDatumVydani())) vystup += " " + this.getDatumVydani();
        vystup = Utils.normalize(vystup);
        return vystup;
    }
    
    public String getFullNakladatel() {
        String vystup = "";
        if (!Utils.jePrazdne(this.getNakladatel())) vystup += this.getNakladatel();
        if (!Utils.jePrazdne(this.getDatumVydani())) vystup += " " + this.getDatumVydani();
        if (!Utils.jePrazdne(this.getEdice())) vystup += " " + this.getEdice();
        if (!Utils.jePrazdne(this.getVydani())) vystup += " " + this.getVydani();
        if (!Utils.jePrazdne(this.getMistoVydani())) vystup += " " + this.getMistoVydani();
        return vystup;
    }

}
