package bixo.storm;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.message.Message;
import kafka.serializer.Encoder;

import org.apache.hadoop.io.DataOutputBuffer;

public class UrlDatumEncoder implements Encoder<UrlDatum> {

    // TODO use threadlocal around DataOutputBuffer, so we're not allocating constantly.
    // Or can we do that in a constructor - are we thread-safe?
    
    @Override
    public Message toMessage(UrlDatum input) {
        DataOutputBuffer out = new DataOutputBuffer(1000);
        try {
            input.write(out);
        } catch (IOException e) {
            throw new RuntimeException("Impossible exception", e);
        }
        
        byte[] data = new byte[out.getLength()];
        System.arraycopy(out.getData(), 0, data, 0, out.getLength());
        Message result = new Message(data);
        return result;
    }

}
