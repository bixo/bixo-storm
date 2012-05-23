package bixo.storm;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import kafka.message.Message;

import org.junit.Test;

public class UrlDatumEncoderTest {

    @Test
    public void test() {
        UrlDatumEncoder encoder = new UrlDatumEncoder();
        
        Message m = encoder.toMessage(new UrlDatum("http://domain.com", "unfatched"));
        assertTrue(m.isValid());
    }

}
