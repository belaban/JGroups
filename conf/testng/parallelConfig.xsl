<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output omit-xml-declaration="yes" indent="yes"/>

    <xsl:param name="parallel" select="'methods'"/>
    <xsl:param name="thread-count" select="'1'"/>

    <!-- copy everything -->
    <xsl:template match="@*|node()">
      <xsl:copy>
	<xsl:apply-templates select="@*|node()"/>
      </xsl:copy>
    </xsl:template>

    <!-- change value of parallel attribute -->
    <xsl:template match="@parallel[parent::suite]">
      <xsl:attribute name="parallel">
	<xsl:value-of select="$parallel"/>
      </xsl:attribute>
    </xsl:template>

    <!-- change value of thread-count attribute -->
    <xsl:template match="@thread-count[parent::suite]">
      <xsl:attribute name="thread-count">
	<xsl:value-of select="$thread-count"/>
      </xsl:attribute>
    </xsl:template>
</xsl:stylesheet>
