<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format"
                version='1.0'>

<!-- ********************************************************************
     $Id: formal.xsl,v 1.1 2003/09/09 01:24:05 belaban Exp $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://nwalsh.com/docbook/xsl/ for copyright
     and other information.

     ******************************************************************** -->

<xsl:template name="formal.object">
  <xsl:variable name="id">
    <xsl:call-template name="object.id"/>
  </xsl:variable>

  <fo:block id="{$id}"
            xsl:use-attribute-sets="formal.object.properties">
    <xsl:call-template name="formal.object.heading"/>
    <xsl:apply-templates/>
  </fo:block>
</xsl:template>

<xsl:template name="formal.object.heading">
  <xsl:param name="title"></xsl:param>
  <fo:block xsl:use-attribute-sets="formal.title.properties">
    <xsl:apply-templates select="." mode="object.title.markup"/>
  </fo:block>
</xsl:template>

<xsl:template name="informal.object">
  <fo:block>
    <xsl:apply-templates/>
  </fo:block>
</xsl:template>

<xsl:template name="semiformal.object">
  <xsl:choose>
    <xsl:when test="./title">
      <xsl:call-template name="formal.object"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:call-template name="informal.object"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="figure">
  <!-- FIXME: is this too careless? -->
  <xsl:choose>
    <xsl:when test=".//imagedata[@align][1]">
      <fo:block text-align="{.//imagedata[@align][1]/@align}">
        <xsl:call-template name="formal.object"/>
      </fo:block>
    </xsl:when>
    <xsl:otherwise>
      <xsl:call-template name="formal.object"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="example">
  <xsl:call-template name="formal.object"/>
</xsl:template>

<xsl:template name="table.frame">
  <xsl:variable name="frame">
    <xsl:choose>
      <xsl:when test="@frame">
        <xsl:value-of select="@frame"/>
      </xsl:when>
      <xsl:otherwise>all</xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="$frame='all'">
      <xsl:attribute name="border-left-style">solid</xsl:attribute>
      <xsl:attribute name="border-right-style">solid</xsl:attribute>
      <xsl:attribute name="border-top-style">solid</xsl:attribute>
      <xsl:attribute name="border-bottom-style">solid</xsl:attribute>
    </xsl:when>
    <xsl:when test="$frame='bottom'">
      <xsl:attribute name="border-left-style">none</xsl:attribute>
      <xsl:attribute name="border-right-style">none</xsl:attribute>
      <xsl:attribute name="border-top-style">none</xsl:attribute>
      <xsl:attribute name="border-bottom-style">solid</xsl:attribute>
    </xsl:when>
    <xsl:when test="$frame='sides'">
      <xsl:attribute name="border-left-style">solid</xsl:attribute>
      <xsl:attribute name="border-right-style">solid</xsl:attribute>
      <xsl:attribute name="border-top-style">none</xsl:attribute>
      <xsl:attribute name="border-bottom-style">none</xsl:attribute>
    </xsl:when>
    <xsl:when test="$frame='top'">
      <xsl:attribute name="border-left-style">none</xsl:attribute>
      <xsl:attribute name="border-right-style">none</xsl:attribute>
      <xsl:attribute name="border-top-style">solid</xsl:attribute>
      <xsl:attribute name="border-bottom-style">none</xsl:attribute>
    </xsl:when>
    <xsl:when test="$frame='topbot'">
      <xsl:attribute name="border-left-style">none</xsl:attribute>
      <xsl:attribute name="border-right-style">none</xsl:attribute>
      <xsl:attribute name="border-top-style">solid</xsl:attribute>
      <xsl:attribute name="border-bottom-style">solid</xsl:attribute>
    </xsl:when>
    <xsl:when test="$frame='none'">
      <xsl:attribute name="border-left-style">none</xsl:attribute>
      <xsl:attribute name="border-right-style">none</xsl:attribute>
      <xsl:attribute name="border-top-style">none</xsl:attribute>
      <xsl:attribute name="border-bottom-style">none</xsl:attribute>
    </xsl:when>
    <xsl:otherwise>
      <xsl:message>
        <xsl:text>Impossible frame on table: </xsl:text>
        <xsl:value-of select="$frame"/>
      </xsl:message>
      <xsl:attribute name="border-left-style">none</xsl:attribute>
      <xsl:attribute name="border-right-style">none</xsl:attribute>
      <xsl:attribute name="border-top-style">none</xsl:attribute>
      <xsl:attribute name="border-bottom-style">none</xsl:attribute>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<xsl:template match="table">
  <xsl:variable name="id">
    <xsl:call-template name="object.id"/>
  </xsl:variable>
  <xsl:variable name="prop-columns"
    select=".//colspec[contains(@colwidth, '*')]"/>

  <fo:block>
    <xsl:attribute name="span">
      <xsl:choose>
        <xsl:when test="@pgwide=1">all</xsl:when>
        <xsl:otherwise>none</xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
    <xsl:if test="@orient='land'">
      <xsl:attribute name="reference-orientation">90</xsl:attribute>
    </xsl:if>

    <fo:table-and-caption id="{$id}"
                          xsl:use-attribute-sets="formal.object.properties"
                          keep-together.within-column="1">
      <fo:table-caption>
        <fo:block xsl:use-attribute-sets="formal.title.properties">
          <xsl:apply-templates select="." mode="object.title.markup"/>
        </fo:block>
      </fo:table-caption>
      <fo:table>
        <xsl:call-template name="table.frame"/>
        <xsl:if test="count($prop-columns) != 0">
          <xsl:attribute name="table-layout">fixed</xsl:attribute>
        </xsl:if>
        <xsl:apply-templates select="tgroup"/>
      </fo:table>
    </fo:table-and-caption>
  </fo:block>
</xsl:template>

<xsl:template match="equation">
  <xsl:call-template name="semiformal.object"/>
</xsl:template>

<xsl:template match="figure/title"></xsl:template>
<xsl:template match="table/title"></xsl:template>
<xsl:template match="example/title"></xsl:template>
<xsl:template match="equation/title"></xsl:template>

<xsl:template match="informalfigure">
  <xsl:call-template name="informal.object"/>
</xsl:template>

<xsl:template match="informalexample">
  <xsl:call-template name="informal.object"/>
</xsl:template>

<xsl:template match="informaltable">
  <xsl:variable name="id">
    <xsl:call-template name="object.id"/>
  </xsl:variable>
  <xsl:variable name="prop-columns"
    select=".//colspec[contains(@colwidth, '*')]"/>

  <fo:block>
    <xsl:attribute name="span">
      <xsl:choose>
        <xsl:when test="@pgwide=1">all</xsl:when>
        <xsl:otherwise>none</xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
    <xsl:if test="@orient='land'">
      <xsl:attribute name="reference-orientation">90</xsl:attribute>
    </xsl:if>

    <fo:table id="{$id}"
              xsl:use-attribute-sets="informal.object.properties">
      <xsl:call-template name="table.frame"/>
      <xsl:if test="count($prop-columns) != 0">
        <xsl:attribute name="table-layout">fixed</xsl:attribute>
      </xsl:if>
      <xsl:apply-templates select="tgroup"/>
    </fo:table>
  </fo:block>
</xsl:template>

<xsl:template match="informalequation">
  <xsl:call-template name="informal.object"/>
</xsl:template>

</xsl:stylesheet>
