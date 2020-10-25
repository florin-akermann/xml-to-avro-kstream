<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:template match="/">
        <main>
            <foo>
                <xsl:value-of select="some/a/foo"/>
            </foo>
            <bar>
                <xsl:value-of select="some/b/bar"/>
            </bar>
        </main>
    </xsl:template>
</xsl:stylesheet>