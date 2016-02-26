package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.gson.Gson;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.kafka.KafkaSimpleExtractor;
import kafka.message.MessageAndOffset;

public class KafkaSimpleJsonExtractor extends KafkaSimpleExtractor implements Extractor<String, byte[]> {

    private static final Gson gson = new Gson();
    private static final Charset CHARSET = StandardCharsets.UTF_8;

    public KafkaSimpleJsonExtractor(WorkUnitState state) {
        super(state);
    }

    @Override
    protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
        long offset = messageAndOffset.offset();

        byte[] keyBytes = getBytes(messageAndOffset.message().key());
        String key = (keyBytes == null) ? "" : new String(keyBytes, CHARSET);

        byte[] payloadBytes = getBytes(messageAndOffset.message().payload());
        String payload = (payloadBytes == null) ? "" : new String(payloadBytes, CHARSET);

        KafkaRecord record = new KafkaRecord(offset, key, payload);

        byte[] decodedRecord = gson.toJson(record).getBytes(CHARSET);
        return decodedRecord;
    }

}
