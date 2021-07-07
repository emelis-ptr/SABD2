package kafkaStream.queryUno;

import flink.queryUno.AccumulatorQueryUno;
import org.apache.kafka.streams.kstream.Initializer;

public class InitializerQueryUno implements Initializer<AccumulatorQueryUno> {
    @Override
    public AccumulatorQueryUno apply() {
        return new AccumulatorQueryUno();
    }
}