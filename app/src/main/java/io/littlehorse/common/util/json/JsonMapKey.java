package io.littlehorse.common.util.json;

// Citations: https://stackoverflow.com/questions/42763298/jackson-keep-references-to-keys-in-map-values-when-deserializing

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation used to indicate that the annotated property shall be deserialized to the map key of
 * the current object. Requires that the object is a deserialized map value.
 * 
 * Note: This annotation is not a standard Jackson annotation. It will only work if this is
 * explicitly enabled in the {@link ObjectMapper}.
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonMapKey {

}