/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.registrdigitalizace.harvest.db;

import java.util.List;
import cz.registrdigitalizace.harvest.Utils;
import java.util.ArrayList;
/**
 *
 * @author mkortus
 */
public class AutorEntry {
    private List<String> autor;
    private String jmeno;
    private String role;
    private String rozmezi;
    private String family;
    private List<String> given;
    private String displayForm;
    private String neidentifikovanaPole;
    private Boolean empty;
    private String fullAutor;
    
    public AutorEntry() {
        this.autor = new ArrayList<String>();
        this.jmeno = "";
        this.role = "";
        this.rozmezi = "";
        this.family = "";
        this.given = new ArrayList<String>();
        this.displayForm = "";
        this.neidentifikovanaPole = "";
        this.empty = false;
        this.fullAutor = "";
    }

    public List<String> getAutor() {
        return this.autor;
    }
    
    public String getAutor(int i) {
        if (i<this.autor.size()) {
            return this.autor.get(i);
        }
        return null;
    }
    public void setAutor(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.autor.addAll(valueLocal);
        }
    }

    public void addAutor(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.autor.add(valueLocal);
        }
    }

    public String getJmeno() {
        return this.jmeno;
    }
    
    public void setJmeno(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.jmeno = valueLocal;
        }
    }

    public String getRole() {
        return this.role;
    }
    
    public void setRole(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.role = valueLocal;
        }
    }

    public String getRozmezi() {
        return this.rozmezi;
    }
    
    public void setRozmezi(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.rozmezi = valueLocal;
        }
    }

    public String getFamily() {
        return this.family;
    }
    
    public void setFamily(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.family = valueLocal;
        }
    }

    public List<String> getGiven() {
        return this.given;
    }
    
    public void setGiven(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.given.addAll(valueLocal);
        }
    }

    public void addGiven(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.given.add(valueLocal);
        }
    }

    public String getDisplayForm() {
        return this.displayForm;
    }
    
    public void setDisplayForm(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.displayForm = valueLocal;
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
    
    public String getFullAutor() {
        String vystup = "";
        for (int i=0; i<this.autor.size(); i++) {
            if (!Utils.jePrazdne(this.autor.get(i))) {
                vystup += this.autor.get(i) + " ";
            }
        }
        if (!Utils.jePrazdne(this.family)) vystup += this.family;
        for (int i=0; i<this.given.size(); i++) {
            if (!Utils.jePrazdne(this.given.get(i))) vystup +=  " " + this.given.get(i);
        }
        if (!Utils.jePrazdne(this.rozmezi)) vystup += ", " + this.rozmezi;
        if (!Utils.jePrazdne(displayForm)) vystup += " (" + this.displayForm + ") ";
        return vystup;
    }

    public String toString() {
        String vystup = "";
        if (!Utils.jePrazdne(getFullAutor())) vystup += "autor: " + getFullAutor();
        if (!Utils.jePrazdne(getRole())) vystup += " - role: " + getRole();

        
        if (!Utils.jePrazdne(getNeindefikovanaPole())) vystup += "\n  neidentifikovaná pole: " + getNeindefikovanaPole();
        return vystup;
    }
    
    public Boolean isEmpty() {
        Boolean vysledek = false;
        if ((Utils.jePrazdne(this.jmeno)) & (Utils.jePrazdne(this.role))) {
            vysledek = true;
            return true;
        }
        return vysledek;
    }

    public String hodnota() {
        return Utils.normalize(getFullAutor());
    }

}
