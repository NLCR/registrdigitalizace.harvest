<?xml version="1.0" encoding="UTF-8"?>
<!--
Copyright (C) 2011 Jan Pokorsky

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

<!--

    Transforms MODS to digobject.xsd

-->

<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:mods="http://www.loc.gov/mods/v3"
        xmlns="http://www.registrdigitalizace.cz/digobject/"
        exclude-result-prefixes="mods" version="1.0">
    
    <xsl:output method="xml" indent="yes" />

    <xsl:template match="/">
        <xsl:apply-templates />
    </xsl:template>
    
    <xsl:template match="mods:modsCollection">
        <!-- ignore everything except fot the first mods -->
        <xsl:apply-templates select="mods:mods[1]" />
    </xsl:template>
    
    <xsl:template match="mods:mods">
        <digobject>
            <!-- build title -->
            <xsl:variable name="issueTitle"><xsl:call-template name="issueTitle"/></xsl:variable>
            <xsl:copy-of select="$issueTitle" />
            
            <xsl:if test="$issueTitle = '' ">
                
                <xsl:variable name="volumeTitle"><xsl:call-template name="volumeTitle"/></xsl:variable>
                <xsl:copy-of select="$volumeTitle" />
                
                <xsl:if test="$volumeTitle = '' ">
                    
                    <xsl:variable name="unitTitle"><xsl:call-template name="unitTitle"/></xsl:variable>
                    <xsl:copy-of select="$unitTitle" />
                    
                    <xsl:if test="$unitTitle = '' ">
                        
                        <xsl:variable name="title"><xsl:call-template name="title"/></xsl:variable>
                        <xsl:copy-of select="$title" />
                        
                    </xsl:if>
                </xsl:if>
            </xsl:if>
            

            <xsl:variable name="isbn" select="mods:identifier[@type='isbn']" />
            <xsl:if test="$isbn != ''">
                <isbn>
                    <xsl:value-of select="$isbn" />
                </isbn>
            </xsl:if>

            <xsl:variable name="issn" select="mods:identifier[@type='issn']" />
            <xsl:if test="$issn != ''">
                <issn>
                    <xsl:value-of select="$issn" />
                </issn>
            </xsl:if>

            <xsl:variable name="ccnb" select="mods:identifier[@type='ccnb']" />
            <xsl:if test="$ccnb != ''">
                <ccnb>
                    <xsl:value-of select="$ccnb" />
                </ccnb>
            </xsl:if>

            <xsl:variable name="sigla" select="mods:location/mods:physicalLocation" />
            <xsl:if test="$sigla != ''">
                <sigla>
                    <xsl:value-of select="$sigla" />
                </sigla>
            </xsl:if>

            <!-- signature -->
            <xsl:apply-templates select="mods:location/mods:shelfLocator" />
            
            <!-- build authors -->
            <xsl:for-each select="mods:name[mods:role/mods:roleTerm[@type='code']='cre']">
                <author>
                    <xsl:call-template name="name"/>
                </author>
            </xsl:for-each>
            
            <!-- build publishers -->
            <xsl:for-each select="mods:originInfo[@transliteration='publisher' or not(@transliteration)][mods:publisher]">
                <publisher>
                    <xsl:call-template name="publisher"/>
                </publisher>
            </xsl:for-each>
            
            <!-- year of issuance -->
            <xsl:choose>
                <xsl:when test="mods:part[mods:detail[@type='volume'] and mods:date!='']">
                    <year>
                        <xsl:value-of select="mods:part/mods:date" />
                    </year>
                </xsl:when>
                <xsl:when test="mods:originInfo[mods:issuance!='continuing']">
                    <xsl:for-each select="mods:originInfo[@transliteration='publisher' or not(@transliteration) and mods:publisher!='' and mods:dateIssued!='' ]">
                        <year>
                            <xsl:value-of select="mods:dateIssued" />
                        </year>
                    </xsl:for-each>
                </xsl:when>
            </xsl:choose>

        </digobject>
    </xsl:template>
        
    <xsl:template match="mods:mods/mods:location/mods:shelfLocator">
        <xsl:if test=". != ''">
            <signature>
                <xsl:value-of select="." />
            </signature>
        </xsl:if>
    </xsl:template>
        
    <xsl:template match="mods:mods/mods:titleInfo">
        <xsl:value-of select="mods:nonSort" />
        <xsl:if test="mods:nonSort != ''">
            <xsl:text> </xsl:text>
        </xsl:if>
        
        <xsl:value-of select="mods:title" />
        
        <xsl:if test="mods:subTitle != ''">
            <xsl:text>: </xsl:text>
            <xsl:value-of select="mods:subTitle" />
        </xsl:if>
        
        <xsl:if test="mods:partNumber != ''">
            <xsl:text>. </xsl:text>
            <xsl:value-of select="mods:partNumber" />
        </xsl:if>
        
        <xsl:if test="mods:partName != ''">
            <xsl:text>. </xsl:text>
            <xsl:value-of select="mods:partName" />
        </xsl:if>
    </xsl:template>
    
    <xsl:template match="*" />
    
    <!-- print main title -->
    <xsl:template name="title">

        <xsl:variable name="title">
            <xsl:for-each select="mods:titleInfo">
                <xsl:variable name="titleitem">
                    <xsl:apply-templates select="." />
                </xsl:variable>

                <xsl:if test="$titleitem != ''">
                    <xsl:text>, </xsl:text>
                    <xsl:value-of select="normalize-space($titleitem)" />
                </xsl:if>
            </xsl:for-each>
        </xsl:variable>

        <xsl:if test="$title != ''">
            <title>
                <!-- trim prefix ", " from title -->
                <xsl:value-of select="substring($title, 3)" />
            </title>
        </xsl:if>
        
    </xsl:template>
    
    <!-- print periodical issue title -->
    <xsl:template name="issueTitle">
        
        <xsl:variable name="issueNumber" select="mods:part/mods:detail[@type='issue']/mods:number" />
        <xsl:if test="$issueNumber != ''">
            <title>
                <xsl:text>Číslo: </xsl:text>
                <xsl:value-of select="normalize-space($issueNumber)" />
            </title>
        </xsl:if>
        
    </xsl:template>
    
    <!-- print periodical volume title -->
    <xsl:template name="volumeTitle">
        
        <xsl:variable name="volumeNumber" select="mods:part/mods:detail[@type='volume']/mods:number" />
        <xsl:if test="$volumeNumber != ''">
            <title>
                <xsl:text>Ročník: </xsl:text>
                <xsl:value-of select="normalize-space($volumeNumber)" />
            </title>
        </xsl:if>
        
    </xsl:template>
    
    <!-- print monograph unit title -->
    <xsl:template name="unitTitle">
        
                <!-- monograph unit title -->
        <xsl:variable name="unitNumber" select="mods:part[@type='Volume']/mods:detail/mods:number" />
        <xsl:if test="$unitNumber != ''">
            <title>
                <xsl:text>Část: </xsl:text>
                <xsl:value-of select="normalize-space($unitNumber)" />
            </title>
        </xsl:if>
        
    </xsl:template>
    
    <!-- print publisher mods:originInfo[mods:publisher]-->
    <xsl:template name="publisher">
        
        <xsl:variable name="publisher">
            <xsl:value-of select="mods:publisher"/>
            <xsl:text> </xsl:text>
            <xsl:value-of select="mods:dateIssued"/>
        </xsl:variable>
        
        <xsl:value-of select="normalize-space($publisher)"/>
        
    </xsl:template>

    <!-- print mods:name -->
    <xsl:template name="name">
        
        <xsl:variable name="name">
            <xsl:for-each select="mods:namePart[not(@type)]">
                <xsl:value-of select="."/>
                <xsl:text> </xsl:text>
            </xsl:for-each>
            <xsl:value-of select="mods:namePart[@type='family']"/>
            
            <xsl:for-each select="mods:namePart[@type='given']">
                <xsl:text> </xsl:text>
                <xsl:value-of select="."/>
            </xsl:for-each>
            
            <xsl:if test="mods:namePart[@type='date']">
                <xsl:text>, </xsl:text>
                <xsl:value-of select="mods:namePart[@type='date']"/>
                <xsl:text/>
            </xsl:if>
            
            <xsl:if test="mods:displayForm">
                <xsl:text> (</xsl:text>
                <xsl:value-of select="mods:displayForm"/>
                <xsl:text>) </xsl:text>
            </xsl:if>
        </xsl:variable>
        
        <xsl:value-of select="normalize-space($name)"/>
    </xsl:template>

</xsl:stylesheet>
