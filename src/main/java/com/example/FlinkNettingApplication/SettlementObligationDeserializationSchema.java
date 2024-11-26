package com.example.FlinkNettingApplication;

import com.example.schema.SettlementObligation;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class SettlementObligationDeserializationSchema extends AbstractDeserializationSchema<SettlementObligation> {
    @Override
    public SettlementObligation deserialize(byte[] message) throws IOException {
        // Use Avro to deserialize the byte array into a SettlementObligation object
        DatumReader<SettlementObligation> reader = new SpecificDatumReader<>(SettlementObligation.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        return reader.read(null, decoder);
    }
}
