package bixo.storm;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DataInputBuffer;

import kafka.message.Message;
import kafka.serializer.Decoder;

public class UrlDatumDecoder implements Decoder<UrlDatum> {

    @Override
    public UrlDatum toEvent(Message msg) {
        UrlDatum result = new UrlDatum();
        
        ByteBuffer bb = msg.payload();
        DataInputBuffer in = new DataInputBuffer();

        // TODO is this really the best option? Allocating a new data array?
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        in.reset(data, data.length);
        
        try {
            result.readFields(in);
        } catch (IOException e) {
            throw new RuntimeException("Exception decoding message into UrlDatum", e);
        }
        
        return result;
    }

}
