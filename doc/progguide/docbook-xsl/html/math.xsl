<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version='1.0'>

<!-- ********************************************************************
     $Id: math.xsl,v 1.1.1.1 2003/09/09 01:24:06 belaban Exp $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://nwalsh.com/docbook/xsl/ for copyright
     and other information.

     ******************************************************************** -->

<xsl:template match="inlineequation">
  <xsl:apply-templates/>
</xsl:template>

<xsl:template match="alt">
</xsl:template>

<!-- Support for TeX math in alt -->

<xsl:template match="*" mode="collect.tex.math">
  <xsl:call-template name="write.text.chunk">
    <xsl:with-param name="filename" select="$tex.math.file"/>
    <xsl:with-param name="method" select="'text'"/>
    <xsl:with-param name="content">
      <xsl:choose>
        <xsl:when test="$tex.math.in.alt = 'plain'">
          <xsl:call-template name="tex.math.plain.head"/>
          <xsl:apply-templates select="." mode="collect.tex.math.plain"/>
          <xsl:call-template name="tex.math.plain.tail"/>
        </xsl:when>
        <xsl:when test="$tex.math.in.alt = 'latex'">
          <xsl:call-template name="tex.math.latex.head"/>
          <xsl:apply-templates select="." mode="collect.tex.math.latex"/>
          <xsl:call-template name="tex.math.latex.tail"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:message>
            Unsupported TeX math notation: 
            <xsl:value-of select="$tex.math.in.alt"/>
          </xsl:message>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:with-param>
    <xsl:with-param name="encoding" select="$default.encoding"/>
  </xsl:call-template>
</xsl:template>

<!-- PlainTeX -->

<xsl:template name="tex.math.plain.head">
  <xsl:text>\nopagenumbers &#xA;</xsl:text>
</xsl:template>

<xsl:template name="tex.math.plain.tail">
  <xsl:text>\bye &#xA;</xsl:text>
</xsl:template>

<xsl:template match="inlineequation" mode="collect.tex.math.plain">
  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="graphic">
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="graphic"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="select.mediaobject.filename">
          <xsl:with-param name="olist" select="inlinemediaobject/*"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:text>\special{dvi2bitmap outputfile </xsl:text>
  <xsl:value-of select="$filename"/>
  <xsl:text>} &#xA;</xsl:text>
  <xsl:text>$</xsl:text>
  <xsl:value-of select="alt[@role='tex'] | inlinemediaobject/textobject[@role='tex']"/>
  <xsl:text>$ &#xA;</xsl:text>
  <xsl:text>\vfill\eject &#xA;</xsl:text>
</xsl:template>

<xsl:template match="equation|informalequation" mode="collect.tex.math.plain">
  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="graphic">
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="graphic"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="select.mediaobject.filename">
          <xsl:with-param name="olist" select="mediaobject/*"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:text>\special{dvi2bitmap outputfile </xsl:text>
  <xsl:value-of select="$filename"/>
  <xsl:text>} &#xA;</xsl:text>
  <xsl:text>$$</xsl:text>
  <xsl:value-of select="alt[@role='tex'] | mediaobject/textobject[@role='tex']"/>
  <xsl:text>$$ &#xA;</xsl:text>
  <xsl:text>\vfill\eject &#xA;</xsl:text>
</xsl:template>

<xsl:template match="text()" mode="collect.tex.math.plain"/>

<!-- LaTeX -->

<xsl:template name="tex.math.latex.head">
  <xsl:text>\documentclass{article} &#xA;</xsl:text>
  <xsl:text>\pagestyle{empty} &#xA;</xsl:text>
  <xsl:text>\begin{document} &#xA;</xsl:text>
</xsl:template>

<xsl:template name="tex.math.latex.tail">
  <xsl:text>\end{document} &#xA;</xsl:text>
</xsl:template>

<xsl:template match="inlineequation" mode="collect.tex.math.latex">
  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="graphic">
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="graphic"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="select.mediaobject.filename">
          <xsl:with-param name="olist" select="inlinemediaobject/*"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:text>\special{dvi2bitmap outputfile </xsl:text>
  <xsl:value-of select="$filename"/>
  <xsl:text>} &#xA;</xsl:text>
  <xsl:text>$</xsl:text>
  <xsl:value-of select="alt[@role='tex'] | inlinemediaobject/textobject[@role='tex']"/>
  <xsl:text>$ &#xA;</xsl:text>
  <xsl:text>\newpage &#xA;</xsl:text>
</xsl:template>

<xsl:template match="equation|informalequation" mode="collect.tex.math.latex">
  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="graphic">
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="graphic"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="select.mediaobject.filename">
          <xsl:with-param name="olist" select="mediaobject/*"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:text>\special{dvi2bitmap outputfile </xsl:text>
  <xsl:value-of select="$filename"/>
  <xsl:text>} &#xA;</xsl:text>
  <xsl:text>$$</xsl:text>
  <xsl:value-of select="alt[@role='tex'] | mediaobject/textobject[@role='tex']"/>
  <xsl:text>$$ &#xA;</xsl:text>
  <xsl:text>\newpage &#xA;</xsl:text>
</xsl:template>

<xsl:template match="text()" mode="collect.tex.math.latex"/>

<!-- Extracting image filename from mediaobject and graphic elements -->

<xsl:template name="select.mediaobject.filename">
  <xsl:param name="olist"
             select="imageobject|imageobjectco
                     |videoobject|audioobject|textobject"/>
  <xsl:param name="count">1</xsl:param>

  <xsl:if test="$count &lt;= count($olist)">
    <xsl:variable name="object" select="$olist[position()=$count]"/>

    <xsl:variable name="useobject">
      <xsl:choose>
	<!-- The phrase is never used -->
        <xsl:when test="name($object)='textobject' and $object/phrase">
          <xsl:text>0</xsl:text>
        </xsl:when>
	<!-- The first textobject is not a reasonable fallback for equation image -->
        <xsl:when test="name($object)='textobject'">
          <xsl:text>0</xsl:text>
        </xsl:when>
	<!-- If there's only one object, use it -->
	<xsl:when test="$count = 1 and count($olist) = 1">
	  <xsl:text>1</xsl:text>
	</xsl:when>
	<!-- Otherwise, see if this one is a useable graphic -->
        <xsl:otherwise>
          <xsl:choose>
            <!-- peek inside imageobjectco to simplify the test -->
            <xsl:when test="local-name($object) = 'imageobjectco'">
              <xsl:call-template name="is.acceptable.mediaobject">
                <xsl:with-param name="object" select="$object/imageobject"/>
              </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
              <xsl:call-template name="is.acceptable.mediaobject">
                <xsl:with-param name="object" select="$object"/>
              </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:choose>
      <xsl:when test="$useobject='1'">
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="$object"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="select.mediaobject">
          <xsl:with-param name="olist" select="$olist"/>
          <xsl:with-param name="count" select="$count + 1"/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:if>
</xsl:template>

</xsl:stylesheet>
