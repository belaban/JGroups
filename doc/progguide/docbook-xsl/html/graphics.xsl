<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xlink="http://www.w3.org/1999/xlink"
                xmlns:stext="http://nwalsh.com/xslt/ext/com.nwalsh.saxon.TextFactory"
                xmlns:xtext="com.nwalsh.xalan.Text"
                xmlns:lxslt="http://xml.apache.org/xslt"
                exclude-result-prefixes="xlink stext xtext lxslt"
                extension-element-prefixes="stext xtext"
                version='1.0'>

<!-- ********************************************************************
     $Id: graphics.xsl,v 1.1.1.1 2003/09/09 01:24:06 belaban Exp $
     ********************************************************************

     This file is part of the XSL DocBook Stylesheet distribution.
     See ../README or http://nwalsh.com/docbook/xsl/ for copyright
     and other information.

     Contributors:
     Colin Paul Adams, <colin@colina.demon.co.uk>

     ******************************************************************** -->

<lxslt:component prefix="xtext"
                 elements="insertfile"/>

<!-- ==================================================================== -->
<!-- Graphic format tests for the HTML backend -->

<xsl:template name="is.graphic.format">
  <xsl:param name="format"></xsl:param>
  <xsl:if test="$format = 'PNG'
                or $format = 'JPG'
                or $format = 'JPEG'
                or $format = 'linespecific'
                or $format = 'GIF'
                or $format = 'GIF87a'
                or $format = 'GIF89a'
                or $format = 'BMP'">1</xsl:if>
</xsl:template>

<xsl:template name="is.graphic.extension">
  <xsl:param name="ext"></xsl:param>
  <xsl:if test="$ext = 'png'
                or $ext = 'jpeg'
                or $ext = 'jpg'
                or $ext = 'avi'
                or $ext = 'mpg'
                or $ext = 'mpeg'
                or $ext = 'qt'
                or $ext = 'gif'
                or $ext = 'bmp'">1</xsl:if>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="screenshot">
  <div class="{name(.)}">
    <xsl:apply-templates/>
  </div>
</xsl:template>

<xsl:template match="screeninfo">
</xsl:template>

<!-- ==================================================================== -->

<xsl:template name="process.image">
  <!-- When this template is called, the current node should be  -->
  <!-- a graphic, inlinegraphic, imagedata, or videodata. All    -->
  <!-- those elements have the same set of attributes, so we can -->
  <!-- handle them all in one place.                             -->
  <xsl:param name="tag" select="'img'"/>
  <xsl:param name="alt"/>
  <xsl:param name="longdesc"/>

  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="local-name(.) = 'graphic'
                      or local-name(.) = 'inlinegraphic'">
        <!-- handle legacy graphic and inlinegraphic by new template --> 
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select="."/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <!-- imagedata, videodata, audiodata -->
        <xsl:call-template name="mediaobject.filename">
          <xsl:with-param name="object" select=".."/>
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="width">
    <xsl:choose>
      <xsl:when test="@scale"><xsl:value-of select="@scale"/>%</xsl:when>
      <xsl:when test="@width">
        <xsl:variable name="w-magnitude">
          <xsl:call-template name="length-magnitude">
            <xsl:with-param name="length" select="@width"/>
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="w-units">
          <xsl:call-template name="length-units">
            <xsl:with-param name="length" select="@width"/>
            <xsl:with-param name="default.units" select="'px'"/>
          </xsl:call-template>
        </xsl:variable>

        <xsl:choose>
          <xsl:when test="$w-units = '%'">
            <xsl:value-of select="@width"/>
          </xsl:when>
          <xsl:when test="$w-units = 'cm'">
            <xsl:value-of select="round(($w-magnitude div 2.54) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$w-units = 'mm'">
            <xsl:value-of select="round(($w-magnitude div 25.4) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$w-units = 'in'">
            <xsl:value-of select="round($w-magnitude * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$w-units = 'pt'">
            <xsl:value-of select="round(($w-magnitude div 72) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$w-units = 'pc'">
            <xsl:value-of select="round(($w-magnitude div 6) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$w-units = 'px'">
            <xsl:value-of select="$w-magnitude"/>
          </xsl:when>
          <xsl:when test="$w-units = 'em'">
            <xsl:message>
              <xsl:text>Relative units (ems) are not supported on widths.  </xsl:text>
              <xsl:text>Using 12pt/em.</xsl:text>
            </xsl:message>
            <xsl:value-of select="round((($w-magnitude * 12) div 72) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:message>
              <xsl:text>Unrecognized unit given for width: </xsl:text>
              <xsl:value-of select="$w-units"/>
              <xsl:text>. Treating as px.</xsl:text>
              <xsl:value-of select="$w-magnitude"/>
            </xsl:message>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise></xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="height">
    <xsl:choose>
      <xsl:when test="@scale"></xsl:when>
      <xsl:when test="@depth">
        <xsl:variable name="d-magnitude">
          <xsl:call-template name="length-magnitude">
            <xsl:with-param name="length" select="@depth"/>
          </xsl:call-template>
        </xsl:variable>
        <xsl:variable name="d-units">
          <xsl:call-template name="length-units">
            <xsl:with-param name="length" select="@depth"/>
            <xsl:with-param name="default.units" select="'px'"/>
          </xsl:call-template>
        </xsl:variable>

        <xsl:choose>
          <xsl:when test="$d-units = '%'">
            <xsl:value-of select="@depth"/>
          </xsl:when>
          <xsl:when test="$d-units = 'cm'">
            <xsl:value-of select="round(($d-magnitude div 2.54) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$d-units = 'mm'">
            <xsl:value-of select="round(($d-magnitude div 25.4) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$d-units = 'in'">
            <xsl:value-of select="round($d-magnitude * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$d-units = 'pt'">
            <xsl:value-of select="round(($d-magnitude div 72) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$d-units = 'pc'">
            <xsl:value-of select="round(($d-magnitude div 6) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:when test="$d-units = 'px'">
            <xsl:value-of select="$d-magnitude"/>
          </xsl:when>
          <xsl:when test="$d-units = 'em'">
            <xsl:message>
              <xsl:text>Relative units (ems) are not supported on depths.  </xsl:text>
              <xsl:text>Using 12pt/em.</xsl:text>
            </xsl:message>
            <xsl:value-of select="round((($d-magnitude * 12) div 72) * $pixels.per.inch)"/>
          </xsl:when>
          <xsl:otherwise>
            <xsl:message>
              <xsl:text>Unrecognized unit given for depth: </xsl:text>
              <xsl:value-of select="$d-units"/>
              <xsl:text>. Treating as px.</xsl:text>
              <xsl:value-of select="$d-magnitude"/>
            </xsl:message>
          </xsl:otherwise>
        </xsl:choose>
      </xsl:when>
      <xsl:otherwise></xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:variable name="align">
    <xsl:value-of select="@align"/>
  </xsl:variable>

  <xsl:element name="{$tag}">
    <xsl:attribute name="src">
      <xsl:value-of select="$filename"/>
    </xsl:attribute>
    <xsl:if test="$align != ''">
      <xsl:attribute name="align">
        <xsl:value-of select="$align"/>
      </xsl:attribute>
    </xsl:if>
    <xsl:if test="$height != ''">
      <xsl:attribute name="height">
        <xsl:value-of select="$height"/>
      </xsl:attribute>
    </xsl:if>
    <xsl:if test="$width != ''">
      <xsl:attribute name="width">
        <xsl:value-of select="$width"/>
      </xsl:attribute>
    </xsl:if>
    <xsl:if test="$alt != ''">
      <xsl:attribute name="alt">
        <xsl:value-of select="$alt"/>
      </xsl:attribute>
    </xsl:if>
    <xsl:if test="$longdesc != ''">
      <xsl:attribute name="longdesc">
        <xsl:value-of select="$longdesc"/>
      </xsl:attribute>
    </xsl:if>
  </xsl:element>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="graphic">
  <p>
    <xsl:call-template name="anchor"/>
    <xsl:call-template name="process.image"/>
  </p>
</xsl:template>

<xsl:template match="inlinegraphic">
  <xsl:variable name="filename">
    <xsl:choose>
      <xsl:when test="@entityref">
        <xsl:value-of select="unparsed-entity-uri(@entityref)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="@fileref"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>

  <xsl:if test="@id">
    <a name="{@id}"/>
  </xsl:if>

  <xsl:choose>
    <xsl:when test="@format='linespecific'">
      <xsl:choose>
        <xsl:when test="$use.extensions != '0'
                        and $textinsert.extension != '0'">
          <xsl:choose>
            <xsl:when test="element-available('stext:insertfile')">
              <stext:insertfile href="{$filename}"/>
            </xsl:when>
            <xsl:when test="element-available('xtext:insertfile')">
              <xtext:insertfile href="{$filename}"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:message terminate="yes">
                <xsl:text>No insertfile extension available.</xsl:text>
              </xsl:message>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>
          <a xlink:type="simple" xlink:show="embed" xlink:actuate="onLoad"
             href="{$filename}"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <xsl:otherwise>
      <xsl:call-template name="process.image"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="mediaobject|mediaobjectco">
  <div class="{name(.)}">
    <xsl:if test="@id">
      <a name="{@id}"/>
    </xsl:if>
    <xsl:call-template name="select.mediaobject"/>
    <xsl:apply-templates select="caption"/>
  </div>
</xsl:template>

<xsl:template match="inlinemediaobject">
  <span class="{name(.)}">
    <xsl:if test="@id">
      <a name="{@id}"/>
    </xsl:if>
    <xsl:call-template name="select.mediaobject"/>
  </span>
</xsl:template>

<xsl:template match="programlisting/inlinemediaobject
                     |screen/inlinemediaobject" priority="2">
  <!-- the additional span causes problems in some cases -->
  <xsl:call-template name="select.mediaobject"/>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="imageobjectco">
  <xsl:if test="@id">
    <a name="{@id}"/>
  </xsl:if>
  <xsl:apply-templates select="imageobject"/>
  <xsl:apply-templates select="calloutlist"/>
</xsl:template>

<xsl:template match="imageobject">
  <xsl:apply-templates select="imagedata"/>
</xsl:template>

<xsl:template match="imagedata">
  <xsl:variable name="filename">
    <xsl:call-template name="mediaobject.filename">
      <xsl:with-param name="object" select=".."/>
    </xsl:call-template>
  </xsl:variable>

  <xsl:choose>
    <xsl:when test="@format='linespecific'">
      <xsl:choose>
        <xsl:when test="$use.extensions != '0'
                        and $textinsert.extension != '0'">
          <xsl:choose>
            <xsl:when test="element-available('stext:insertfile')">
              <stext:insertfile href="{$filename}"/>
            </xsl:when>
            <xsl:when test="element-available('xtext:insertfile')">
              <xtext:insertfile href="{$filename}"/>
            </xsl:when>
            <xsl:otherwise>
              <xsl:message terminate="yes">
                <xsl:text>No insertfile extension available.</xsl:text>
              </xsl:message>
            </xsl:otherwise>
          </xsl:choose>
        </xsl:when>
        <xsl:otherwise>
          <a xlink:type="simple" xlink:show="embed" xlink:actuate="onLoad"
             href="{$filename}"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <xsl:otherwise>
      <xsl:variable name="longdesc.uri">
        <xsl:call-template name="longdesc.uri">
          <xsl:with-param name="mediaobject"
                          select="ancestor::imageobject/parent::*"/>
        </xsl:call-template>
      </xsl:variable>

      <xsl:call-template name="process.image">
        <xsl:with-param name="alt">
          <xsl:apply-templates select="(../../textobject[not(@role) or @role!='tex']/phrase)[1]"/>
        </xsl:with-param>
        <xsl:with-param name="longdesc">
          <xsl:call-template name="write.longdesc">
            <xsl:with-param name="mediaobject"
                            select="ancestor::imageobject/parent::*"/>
          </xsl:call-template>
        </xsl:with-param>
      </xsl:call-template>

      <xsl:if test="$html.longdesc != 0 and $html.longdesc.link != 0
                    and ancestor::imageobject/parent::*/textobject[not(phrase)]">
        <xsl:call-template name="longdesc.link">
          <xsl:with-param name="longdesc.uri" select="$longdesc.uri"/>
        </xsl:call-template>
      </xsl:if>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template name="longdesc.uri">
  <xsl:param name="mediaobject" select="."/>

  <xsl:if test="$html.longdesc">
    <xsl:if test="$mediaobject/textobject[not(phrase)]">
      <xsl:variable name="image-id">
        <xsl:call-template name="object.id">
          <xsl:with-param name="object" select="$mediaobject"/>
        </xsl:call-template>
      </xsl:variable>
      <xsl:variable name="filename">
        <xsl:call-template name="make-relative-filename">
          <xsl:with-param name="base.dir" select="$base.dir"/>
          <xsl:with-param name="base.name"
                          select="concat('ld-',$image-id,$html.ext)"/>
        </xsl:call-template>
      </xsl:variable>

      <xsl:value-of select="$filename"/>
    </xsl:if>
  </xsl:if>
</xsl:template>

<xsl:template name="write.longdesc">
  <xsl:param name="mediaobject" select="."/>
  <xsl:if test="$html.longdesc != 0 and $mediaobject/textobject[not(phrase)]">
    <xsl:variable name="filename">
      <xsl:call-template name="longdesc.uri">
        <xsl:with-param name="mediaobject" select="$mediaobject"/>
      </xsl:call-template>
    </xsl:variable>

    <xsl:value-of select="$filename"/>

    <xsl:call-template name="write.chunk">
      <xsl:with-param name="filename" select="$filename"/>
      <xsl:with-param name="content">
        <html>
          <head>
            <title>Long Description</title>
          </head>
          <body>
            <xsl:call-template name="body.attributes"/>
            <xsl:for-each select="$mediaobject/textobject[not(phrase)]">
              <xsl:apply-templates select="./*"/>
            </xsl:for-each>
          </body>
        </html>
      </xsl:with-param>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<xsl:template name="longdesc.link">
  <xsl:param name="longdesc.uri" select="''"/>
  <div class="longdesc-link" align="right">
    <br clear="all"/>
    <span style="font-size: 8pt;">
      <xsl:text>[</xsl:text>
      <a href="{$longdesc.uri}" target="longdesc">D</a>
      <xsl:text>]</xsl:text>
    </span>
  </div>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="videoobject">
  <xsl:apply-templates select="videodata"/>
</xsl:template>

<xsl:template match="videodata">
  <xsl:call-template name="process.image">
    <xsl:with-param name="tag" select="'embed'"/>
    <xsl:with-param name="alt">
      <xsl:apply-templates select="(../../textobject/phrase)[1]"/>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="audioobject">
  <xsl:apply-templates select="audiodata"/>
</xsl:template>

<xsl:template match="audiodata">
  <xsl:call-template name="process.image">
    <xsl:with-param name="tag" select="'embed'"/>
    <xsl:with-param name="alt">
      <xsl:apply-templates select="(../../textobject/phrase)[1]"/>
    </xsl:with-param>
  </xsl:call-template>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="textobject">
  <xsl:apply-templates/>
</xsl:template>

<!-- ==================================================================== -->

<xsl:template match="caption">
  <div class="{name(.)}">
    <xsl:apply-templates/>
  </div>
</xsl:template>

</xsl:stylesheet>
