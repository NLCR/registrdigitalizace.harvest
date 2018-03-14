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
public class KrameriusEntry {
    private String id;
    private String uuid;
    private String druhDokumentu;
    //private String datumVydani;
    private String signatura;
    private String genre;
    private String carKod;
    private String pole001;
    private String sigla;
    private String issueTitle;
    private String volumeTitle;
    private String unitTitle;
    private String katalog;
    private String siglaBibUdaju;
    private String cisloPeriodika;
    private String libraryId;
    private String dListValue;
    private List<String> ccnb;
    private List<String> urnnbn;
    private List<String> isbn;
    private List<String> issn;
    private List<String> nepccnb;
    private List<String> nepisbn;
    private List<String> nepissn;
    private List<String> datumVydani;
    private List<String> oclc;
    private List<NazevEntry> nazev;
    private List<AutorEntry> autor;
    private List<NakladatelEntry> nakladatel;
    
    public KrameriusEntry() {
        this.id = "";
        this.uuid = "";
        this.druhDokumentu = "";
        //this.datumVydani = "";
        this.signatura = "";
        this.genre = "";
        this.carKod = "";
        this.pole001 = "";
        this.sigla = "";
        this.issueTitle = "";
        this.volumeTitle = "";
        this.unitTitle = "";
        this.katalog = "";
        this.siglaBibUdaju = "";
        this.cisloPeriodika = "";
        this.libraryId = "";
        this.dListValue = "";
        this.ccnb = new ArrayList<String>();
        this.urnnbn = new ArrayList<String>();
        this.isbn = new ArrayList<String>();
        this.issn = new ArrayList<String>();
        this.nepccnb = new ArrayList<String>();
        this.nepisbn = new ArrayList<String>();
        this.nepissn = new ArrayList<String>();
        this.datumVydani = new ArrayList<String>();
        this.oclc = new ArrayList<String>();
        this.nazev = new ArrayList<NazevEntry>();
        this.autor = new ArrayList<AutorEntry>();
        this.nakladatel = new ArrayList<NakladatelEntry>();
        
    }
    
    public String getId() {
        return this.id;
    }
    
    public void setId(String valueLocal) {
        if ((!Utils.jePrazdne(valueLocal)) && (!"0".equals(valueLocal))) {
            this.id = valueLocal;
        }
    }

    public String getLibraryId() {
        return this.libraryId;
    }
    
    public void setLibraryId(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.libraryId = valueLocal;
        }
    }

    public String getDListValue() {
        return this.dListValue;
    }
    
    public void setDListValue(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.dListValue = valueLocal;
        }
    }

    public String getUuid() {
        return this.uuid;
    }
    
    public void setUuid(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.uuid = valueLocal;
        }
    }

    public String getDruhDokumentu() {
        return this.druhDokumentu;
    }
    
    public void setDruhDokumentu(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.druhDokumentu = valueLocal;
        }
    }

    public String getIssueTitle() {
        return this.issueTitle;
    }
    
    public void setIssueTitle(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.issueTitle = valueLocal;
        }
    }

    public String getVolumeTitle() {
        return this.volumeTitle;
    }
    
    public void setVolumeTitle(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.volumeTitle = valueLocal;
        }
    }

    public String getUnitTitle() {
        return this.unitTitle;
    }
    
    public void setUnitTitle(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.unitTitle = valueLocal;
        }
    }
    
    public String getKatalog() {
        return this.katalog;
    }
    
    public void setKatalog(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.katalog = valueLocal;
        }
    }
    
    public String getSiglaBibUdaju() {
        return this.siglaBibUdaju;
    }
    
    public void setSiglaBibUdaju(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.siglaBibUdaju = valueLocal;
        }
    }
    
    public String getCisloPeriodika() {
        return this.cisloPeriodika;
    }
    
    public void setCisloPeriodika(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.cisloPeriodika = valueLocal;
        }
    }
    
    public /*String*/List<String> getDatumVydani() {
        return this.datumVydani;
    }
    
    public void setDatumVydani(/*String*/List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.datumVydani.addAll(valueLocal);
        }
    }

    public void addRokVydani(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.datumVydani.add(valueLocal);
        }
    }

    public String getGenre() {
        return this.genre;
    }
    
    public void setGenre(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.genre = valueLocal;
        }
    }

    public String getCarKod() {
        return this.carKod;
    }
    
    public void setCarKod(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.carKod = valueLocal;
        }
    }

    public String getPole001() {
        return this.pole001;
    }
    
    public void setPole001(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.pole001 = valueLocal;
        }
    }

    public String getSigla() {
        return this.sigla;
    }
    
    public void setSigla(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.sigla = valueLocal;
        }
    }

    public List<String> getCcnb() {
        return this.ccnb;
    }
    
    public String getCcnb(int i) {
        if (i<this.ccnb.size()) {
            return this.ccnb.get(i);
        }
        return null;
    }
    
    public void setCcnb(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.ccnb.addAll(valueLocal);
        }
    }

    public void addCcnb(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.ccnb.add(valueLocal);
        }
    }

    public List<String> getUrnnbn() {
        return this.urnnbn;
    }
    
    public String getUrnnbn(int i) {
        if (i<this.urnnbn.size()) {
            return this.urnnbn.get(i);
        }
        return null;
    }
    
    public void setUrnnbn(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.urnnbn.addAll(valueLocal);
        }
    }

    public void addUrnnbn(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.urnnbn.add(valueLocal);
        }
    }

    public List<String> getIsbn() {
        return this.isbn;
    }
    
    public String getIsbn(int i) {
        if (i<this.isbn.size()) {
            return this.isbn.get(i);
        }
        return null;
    }
    
    public void setIsbn(List<String> text) {
        this.isbn.addAll(text);
    }

    public void addIsbn(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.isbn.add(valueLocal);
        }
    }

    public List<String> getIssn() {
        return this.issn;
    }
    
    public String getIssn(int i) {
        if (i<this.issn.size()) {
            return this.issn.get(i);
        }
        return null;
    }
    
    public void setIssn(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.issn.addAll(valueLocal);
        }
    }

    public void addIssn(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.issn.add(valueLocal);
        }
    }

    public List<String> getOclc() {
        return this.oclc;
    }
    
    public String getOclc(int i) {
        if (i<this.oclc.size()) {
            return this.oclc.get(i);
        }
        return null;
    }
    
    public void setOclc(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.oclc.addAll(valueLocal);
        }
    }

    public void addOclc(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.oclc.add(valueLocal);
        }
    }

    public List<String> getNepCcnb() {
        return this.nepccnb;
    }
    
    public String getNepCcnb(int i) {
        if (i<this.nepccnb.size()) {
            return this.nepccnb.get(i);
        }
        return null;
    }
    
    public void setNepCcnb(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nepccnb.addAll(valueLocal);
        }
    }

    public void addNepCcnb(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nepccnb.add(valueLocal);
        }
    }

    public List<String> getNepIsbn() {
        return this.nepisbn;
    }
    
    public String getNepIsbn(int i) {
        if (i<this.nepisbn.size()) {
            return this.nepisbn.get(i);
        }
        return null;
    }
    
    public void setNepIsbn(List<String> text) {
        this.nepisbn.addAll(text);
    }

    public void addNepIsbn(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nepisbn.add(valueLocal);
        }
    }

    public List<String> getNepIssn() {
        return this.nepissn;
    }
    
    public String getNepIssn(int i) {
        if (i<this.nepissn.size()) {
            return this.nepissn.get(i);
        }
        return null;
    }
    
    public void setNepIssn(List<String> valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nepissn.addAll(valueLocal);
        }
    }

    public void addNepIssn(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.nepissn.add(valueLocal);
        }
    }
    
    public List<NazevEntry> getNazev() {
        return this.nazev;
    }
    
    public NazevEntry getNazev(int i) {
        if (i<this.nazev.size()) {
            return this.nazev.get(i);
        }
        return null;
    }
    
    public void setNazev(List<NazevEntry> valueLocal) {
        if ((valueLocal!=null) && (valueLocal.size()>0) && (valueLocal.get(0).toString()!="")) {
            this.nazev.addAll(valueLocal);
        }
    }

    public void addNazev(NazevEntry valueLocal) {
        if ((valueLocal!=null) && (valueLocal.toString()!=null)) {
            this.nazev.add(valueLocal);
        }
    }

    public List<AutorEntry> getAutor() {
        return this.autor;
    }
    
    public AutorEntry getAutor(int i) {
        if (i<this.autor.size()) {
            return this.autor.get(i);
        }
        return null;
    }
    
    public void setAutor(List<AutorEntry> valueLocal) {
        if ((valueLocal!=null) && (valueLocal.size()>0) && (valueLocal.get(0).toString()!="")) {
            this.autor.addAll(valueLocal);
        }
    }

    public void addAutor(AutorEntry valueLocal) {
        if ((valueLocal!=null) && (valueLocal.toString()!=null)) {
            this.autor.add(valueLocal);
        }
    }

    public List<NakladatelEntry> getNakladatel() {
        return this.nakladatel;
    }
    
    public NakladatelEntry getNakladatel(int i) {
        if (i<this.nakladatel.size()) {
            return this.nakladatel.get(i);
        }
        return null;
    }
    
    public void setNakladatel(List<NakladatelEntry> valueLocal) {
        if ((valueLocal!=null) && (valueLocal.size()>0) && (valueLocal.get(0).toString()!="")) {
            this.nakladatel.addAll(valueLocal);
        }
    }

    public void addNakladatel(NakladatelEntry valueLocal) {
        if ((valueLocal!=null) && (valueLocal.toString()!=null)) {
            this.nakladatel.add(valueLocal);
        }
    }
    
    public String getSignatura() {
        return this.signatura;
    }
    
    public void setSignatura(String valueLocal) {
        if (!Utils.jePrazdne(valueLocal)) {
            this.signatura = valueLocal;
        }
    }

    public String toString() {
        String vystup = "";
        if (!Utils.jePrazdne(getUuid())) vystup += "uuid: " + getUuid();
        if (!Utils.jePrazdne(getDruhDokumentu())) vystup += "\ntype: " + getDruhDokumentu();
        if (!Utils.jePrazdne(getSigla())) vystup += "\nsigla: " + getSigla();
        if (!Utils.jePrazdne(getDatumVydani())) vystup += "\ndatumVydani: " + getDatumVydani();
        if (!Utils.jePrazdne(getSignatura())) vystup += "\nsignatura: " + getSignatura();
        if (!Utils.jePrazdne(getGenre())) vystup += "\ngenre: " + getGenre();
        if (!Utils.jePrazdne(getCarKod())) vystup += "\ncarKod: " + getCarKod();
        if (!Utils.jePrazdne(getPole001())) vystup += "\npole001: " + getPole001();

        if (!Utils.jePrazdne(getCcnb())) vystup += "\nccnb: " + listToString(getCcnb());
        if (!Utils.jePrazdne(getUrnnbn())) vystup += "\nurnnbn: " + listToString(getUrnnbn());
        if (!Utils.jePrazdne(getIsbn())) vystup += "\nisbn: " + listToString(getIsbn());
        if (!Utils.jePrazdne(getIssn())) vystup += "\nissn: " + listToString(getIssn());
        if (!Utils.jePrazdne(getNepCcnb())) vystup += "\nnepccnb: " + listToString(getNepCcnb());
        if (!Utils.jePrazdne(getNepIsbn())) vystup += "\nnepisbn: " + listToString(getNepIsbn());
        if (!Utils.jePrazdne(getNepIssn())) vystup += "\nnepissn: " + listToString(getNepIssn());

        if ((getNazev()!=null) && (!getNazev().isEmpty())) vystup += "\nnazev: " + nazevListToString(getNazev());
        if ((getAutor()!=null) && (!getAutor().isEmpty())) vystup += "\nautor: " + autorListToString(getAutor());
        if ((getNakladatel()!=null) && (!getNakladatel().isEmpty())) vystup += "\nnakladatel: " + nakladatelListToString(getNakladatel());

                
        return vystup;
    }

    private String listToString(List<String> hodnoty) {
        String vystup = "";
        if ((hodnoty!=null) && (hodnoty.size()>0)) {
            for (int i=0; i<hodnoty.size(); i++) {
                if (i>0) {
                    vystup += ", ";
                }
                vystup += hodnoty.get(i);
            }
        }
        return vystup;
    }

    private String nazevListToString(List<NazevEntry> hodnoty) {
        String vystup = "";
        if ((hodnoty!=null) && (hodnoty.size()>0)) {
            for (int i=0; i<hodnoty.size(); i++) {
                if (i==0) vystup += "\n.." + hodnoty.size() + "\n";
                if (i>0) {
                    vystup += "\n ";
                }
                vystup += hodnoty.get(i).toString();
            }
        }
        return vystup;
    }

    private String autorListToString(List<AutorEntry> hodnoty) {
        String vystup = "";
        if ((hodnoty!=null) && (hodnoty.size()>0)) {
            for (int i=0; i<hodnoty.size(); i++) {
                if (i==0) vystup += "\n.." + hodnoty.size() + "\n";
                if (i>0) {
                    vystup += "\n ";
                }
                vystup += hodnoty.get(i).toString();
            }
        }
        return vystup;
    }

    private String nakladatelListToString(List<NakladatelEntry> hodnoty) {
        String vystup = "";
        if ((hodnoty!=null) && (hodnoty.size()>0)) {
            for (int i=0; i<hodnoty.size(); i++) {
                if (i==0) vystup += "\n.." + hodnoty.size() + "\n";
                if (i>0) {
                    vystup += "\n ";
                }
                vystup += hodnoty.get(i).toString();
            }
        }
        return vystup;
    }

}
