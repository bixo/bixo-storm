package bixo.storm;

import static org.junit.Assert.*;
import kafka.message.Message;

import org.junit.Test;

public class UrlDatumDecoderTest {

    @Test
    public void testRoundTrip() {
        UrlDatumEncoder encoder = new UrlDatumEncoder();
        
        final String url = "http://domain.com";
        final String status = "unfetched";
        Message m = encoder.toMessage(new UrlDatum(url, status));
        
        UrlDatumDecoder decoder = new UrlDatumDecoder();
        UrlDatum result = decoder.toEvent(m);
        
        assertEquals(url, result.getUrl());
        assertEquals(status, result.getStatus());
    }

}
