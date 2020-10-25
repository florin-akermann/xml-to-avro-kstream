package ch.roboinvest.xml.to.avro.kstream.util;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Pair<K, V> {
    K key;
    V value;
}
