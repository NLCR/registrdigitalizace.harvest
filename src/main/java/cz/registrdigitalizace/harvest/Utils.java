/*
 * Copyright (C) 2012 Jan Pokorsky
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cz.registrdigitalizace.harvest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.w3c.dom.Node;

/**
 * Various helpers.
 *
 * @author Jan Pokorsky
 */
public final class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class.getName());

    public static String elapsedTime(long time) {
        long msecs = time % 1000;
        long secs = time % (1000 * 60) / 1000;
        long mins = time % (1000 * 60 * 60) / 1000 / 60;
        long hours = time % (1000 * 60 * 60 * 24) / 1000 / 60 / 60;
        return String.format("%02d:%02d:%02d.%03d", hours, mins, secs, msecs);
    }

    public static String toString(List<String> messages, String delimiter) {
        StringBuilder sb = new StringBuilder();
        for (String s : messages) {
            if (sb.length() > 0) {
                sb.append(delimiter);
            }
            sb.append(s);
        }
        return sb.toString();
    }

    public static String deDuplikace(String vstup) {
        String vystup = vstup;
        List<String> idsList = new ArrayList<String>();
        try {
            if ((vstup.length()>1) && (vstup.endsWith(","))) {
                vstup = vstup.substring(0,vstup.length()-1);
            }
            String[] ids = vstup.split("\\s*,\\s*");
            for (String id : ids) {
                if ((idsList.isEmpty()) && (!id.isEmpty())) {
                    idsList.add(id);
                } else if ((!idsList.contains(id)) && (!id.isEmpty())) {
                    idsList.add(id);
                }
            }
            String vystupLocal = "";
            try {
                if (!idsList.isEmpty()) {
                    for (String s : idsList) {
                        vystupLocal += ","+s;
                    }
                    if ((vystupLocal.length()>1) && (vystupLocal.startsWith(","))) {
                        vystupLocal = vystupLocal.substring(1);
                    }
                    if ((vystupLocal.length()>1) && (vystupLocal.endsWith(","))) {
                        vystupLocal = vystupLocal.substring(0,vystupLocal.length()-1);
                    }
                }
            } catch (Exception ex) {
                System.out.println(" chyba se stala pri deDuplikaci: " + ex.getMessage());
            }
            if (!"".equals(vystupLocal)) {
                vystup = vystupLocal;
            }
        } catch (Exception ex) {
            System.out.println(" chyba se stala pri deDuplikaci: " + ex.getMessage());
        }
        return vystup;
    }
    
    public static Boolean jePrazdne(String valueStr) {
        Boolean vysledek = false;
        if ((valueStr==null) || (valueStr.isEmpty()))  {
            return true;
        }
        return vysledek;
    }

    public static Boolean jePrazdne(List<String> valueStr) {
        Boolean vysledek = false;
        if ((valueStr==null) || (valueStr.isEmpty()) || (valueStr.size()==0))  {
            return true;
        }
        return vysledek;
    }
    
    public static Boolean jePrazdne(Boolean valueStr) {
        Boolean vysledek = false;
        if ((valueStr==null) || (!valueStr.booleanValue()))  {
            return true;
        }
        return vysledek;
    }
    
    public static String vratString(Node input) {
        if (!jePrazdne(input.getTextContent())) {
            return input.getTextContent();
        } else if (!jePrazdne(input.getNodeValue())) {
            return input.getNodeValue();
        }
        return null;
    }

    public static String normalize(String vstup) {
        return vstup.trim().replace("  ", " ");
    }

    public static String toJson(Object vstup){
        String vystup = "";
        ObjectMapper mapper = new ObjectMapper();
        try {
            vystup = mapper.writeValueAsString(vstup);
        } catch (JsonProcessingException ex) {
            System.out.println("chyba konverze na JSOU: " + ex.getMessage());
        }
        
        return vystup;
    }
    
    public static String ListToString(List<String> seznamHodnot) {
        String vystup = "";
        if (!jePrazdne(seznamHodnot)) {
            for (int i=0; i<seznamHodnot.size(); i++) {
                if (i>0) vystup = vystup + "', '";
                vystup = vystup + seznamHodnot.get(i);
            }
        }
        return vystup;
    }

    /**
     * vrací třídu, metodu a číslo řádku, kde došlo k chybě
     * @return 
     */
    public static String lineNumber() {
        StackTraceElement sTE = Thread.currentThread().getStackTrace()[2];
        return " [ class: " + sTE.getClassName() + ", method: " + sTE.getMethodName() + ", line: " + sTE.getLineNumber() + "]";
    }
    

    /**
     * vrací jméno aktuální metody
     * @return 
     */
    public static String getCurrentMethodName() {
        return Thread.currentThread().getStackTrace()[2].getClassName() + "." + Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public static void tryClose(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, " chyba při uzavření connection", ex);
            }
        }
    }

    public static void tryClose(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, " chyba při uzavření statement", ex);
            }
        }
    }

    public static void tryClose(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                LOG.log(Level.SEVERE, " chyba při uzavření resourceset", ex);
            }
        }
    }
    
    public static int parseInteger(String value) {
        if (value==null) {
            return 0;
        } else {
            return Integer.parseInt(value);
        }
    }

}
