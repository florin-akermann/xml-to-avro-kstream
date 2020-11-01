package ch.roboinvest.xml.to.avro.kstream.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.With;
import lombok.experimental.Accessors;


@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Envelope<V>{

    @With
    private final V value;
    @With
    private final Exception exception;

    private boolean isValid;

    @With
    @Accessors(fluent = true)
    private boolean validationApplied;

    public Envelope(V value, Exception exception) {
        this.value = value;
        this.exception = exception;
    }

    public Envelope(V value) {
        this.value = value;
        this.exception = null;
    }

    public Envelope<V> withAdditionalException(Exception throwable){
        if (this.exception == null){
            return this.withException(throwable);
        }else{
            Envelope<V> copy = withValue(value);
            if (copy.exception != null) {
                copy.exception.addSuppressed(throwable);
            }
            return copy;
        }
    }

    public boolean success(){
        return exception == null;
    }

    public Envelope<V> withIsValid(boolean isValid){
        this.isValid = isValid;
        return this.withValidationApplied(true);
    }
}
