package org.oracle.okafka.connect.common.utils;

import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class AppInfoParserTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void testVersion()
    {
        assertNotEquals("unknown", AppInfoParser.getVersion());
    }
}
