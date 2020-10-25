package ch.roboinvest.xml.to.avro.kstream.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.With;


@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Envelope<V>{

    @With
    private final V value;
    @With
    private final Exception exception;

    private boolean isValid;

    @With
    private boolean hasBeenValidated;

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
            return copy.withAdditionalException(throwable);
        }
    }

    public boolean success(){
        return exception == null;
    }

    public boolean validationSuccessful(){
        return hasBeenValidated && isValid;
    }

    public Envelope<V> withIsValid(boolean isValid){
        this.isValid = isValid;
        return this.withHasBeenValidated(true);
    }
}
