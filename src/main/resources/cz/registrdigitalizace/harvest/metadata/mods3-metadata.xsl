<?xml version="1.0" encoding="UTF-8"?>
<!--
Copyright (C) 2015 Jan Pokorsky

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
-->

<!--
    Transforms MODS to cz.registrdigitalizace.harvest.db.Metadata
-->

<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:mods="http://www.loc.gov/mods/v3"
        xmlns:local="local"
        exclude-result-prefixes="mods local" version="1.0">

    <xsl:output method="xml" indent="yes" />
    <!--<xsl:strip-space elements="*" />-->

    <xsl:template match="/">
        <xsl:text>
</xsl:text>
        <metadata>
            <xsl:apply-templates select="mods:mods | mods:modsCollection/mods:mods[1]" />
        </metadata>
    </xsl:template>

    <xsl:template match="mods:identifier[text()]">
        <xsl:variable name="type" select="translate(@type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')" />
        <xsl:variable name="reliefName" select="document('')//local:idTypeMap/local:entry[@key=$type]" />
        <xsl:if test="$reliefName">
            <item reliefName="{$reliefName}">
                <xsl:if test="@invalid">
                    <xsl:attribute name="invalid">true</xsl:attribute>
                </xsl:if>
                <xsl:value-of select="normalize-space(text())" />
            </item>
        </xsl:if>
    </xsl:template>

    <xsl:template match="mods:recordIdentifier[@source]">
        <item reliefName="katalog">
            <xsl:value-of select="normalize-space(@source)" />
        </item>
        <item reliefName="pole001">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:physicalLocation[text()]">
        <item reliefName="siglaFyzJednotky">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:shelfLocator[text()]">
        <item reliefName="signatura">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:recordContentSource[text()]">
        <item reliefName="siglaBibUdaju">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:name[@type='personal']/mods:namePart[(not(@type) or @type='family' or @type='given') and text()]">
        <item reliefName="osoba">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:titleInfo/mods:title[text()]">
        <item reliefName="nazev">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:titleInfo/mods:partNumber[text()]">
        <item reliefName="cislo">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="mods:originInfo/mods:dateIssued[not(@point) and text()]">
        <item reliefName="rokVydani">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <!--K4 volume/issue dateIssued-->
    <xsl:template match="mods:part/mods:date[not(@point) and text()]">
        <xsl:if test="../mods:detail[@type='volume' or @type='issue']">
            <item reliefName="rokVydani">
                <xsl:value-of select="normalize-space(text())" />
            </item>
        </xsl:if>
    </xsl:template>

    <!--K4 issue partNumber-->
    <xsl:template match="mods:part[@type='PeriodicalIssue']/mods:detail[@type='issue']/mods:number[text()]">
        <item reliefName="cislo">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <!--K4 volume partNumber-->
    <xsl:template match="mods:part[not(@type)]/mods:detail[@type='volume']/mods:number[text()]">
        <item reliefName="cislo">
            <xsl:value-of select="normalize-space(text())" />
        </item>
    </xsl:template>

    <xsl:template match="text()|@*">
    </xsl:template>

    <!-- map of mods:identifier@type to reliefName -->
    <local:idTypeMap>
        <local:entry key="barcode">barcode</local:entry>
        <local:entry key="ccnb">ccnb</local:entry>
        <local:entry key="oclc">oclc</local:entry>
        <local:entry key="urnnbn">urnnbn</local:entry>
        <local:entry key="isbn">isbn</local:entry>
        <local:entry key="issn">issn</local:entry>
    </local:idTypeMap>

</xsl:stylesheet>
