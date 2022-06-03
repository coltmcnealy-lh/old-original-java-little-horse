package io.littlehorse.common.util.json;

// Citations: https://stackoverflow.com/questions/42763298/jackson-keep-references-to-keys-in-map-values-when-deserializing

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

public class JsonMapKeyDeserializer extends DelegatingDeserializer {

    private static final long serialVersionUID = 1L;
    private BeanDescription beanDescription;

    public JsonMapKeyDeserializer(JsonDeserializer<?> delegate, BeanDescription beanDescription) {
        super(delegate);
        this.beanDescription = beanDescription;
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
        return new JsonMapKeyDeserializer(newDelegatee, beanDescription);
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String mapKey = p.getCurrentName();
        Object deserializedObject = super.deserialize(p, ctxt);

        // set map key on all fields annotated with @JsonMapKey
        for (BeanPropertyDefinition beanProperty : beanDescription.findProperties()) {
            AnnotatedField field = beanProperty.getField();
            if (field != null && field.getAnnotation(JsonMapKey.class) != null) {
                field.setValue(deserializedObject, mapKey);
            }
        }
        return deserializedObject;
    }
}