package io.littlehorse.common.model.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import io.littlehorse.common.exceptions.VarSubError;
import io.littlehorse.common.model.BaseSchema;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.proto.VariableTypePb;
import io.littlehorse.proto.VariableValuePb;

/**
 * This is the initial prototype attempt at implementing a form of "typed" variable
 * that is serializable across workflows.
 */
public class VariableValue extends BaseSchema {
    // `type` and `serializedVal` are the ONLY two things that should show up in the
    // serialized version of the class.
    private LHVarType type;
    private byte[] serializedVal; // For now we just use json serialization

    // Constructors
    public VariableValue() {}

    public VariableValue(Object val) throws VarSubError {
        if (val != null) {
            type = getTypeEnumFromClass(val.getClass());
            this.setValue(val);
        }
    }

    public static VariableValue fromProto(VariableValuePb proto) throws VarSubError {
        VariableValue out = new VariableValue();
        out.setSerializedVal(proto.getSerializedValue().toByteArray());
        String typeName = proto.getType().name();
        if (typeName.equals("STRING")) {
            out.setType(LHVarType.STRING);
        } else if (typeName.equals("INT")) {
            out.setType(LHVarType.INT);
        } else if (typeName.equals("DOUBLE")) {
            out.setType(LHVarType.DOUBLE);
        } else if (typeName.equals("BOOLEAN")) {
            out.setType(LHVarType.BOOLEAN);
        } else if(typeName.equals("OBJECT")) {
            out.setType(LHVarType.OBJECT);
        } else {
            out.setType(LHVarType.ARRAY);
        }
        return out;
    }

    public VariableValuePb.Builder toProtoBuilder() {
        VariableValuePb.Builder out = VariableValuePb.newBuilder();
        out.setType(VariableTypePb.valueOf(type.toString()));
        out.setSerializedValue(ByteString.copyFrom(serializedVal));
        return out;
    }

    public byte[] getSerializedVal() {
        return serializedVal;
    }

    public void setSerializedVal(byte[] newVal) throws VarSubError {
        if (newVal == null) {
            type = null;
        }

        // could be null during construction by a JavaBeans-style library, such as
        // Jackson for JSON/Avro serialization/deserialization
        if (type != null) {
            
        }

        serializedVal = newVal;
    }

    public LHVarType getType() {
        return type;
    }

    // Only to be called by Jackson
    public void setType(LHVarType type) {
        if (this.type != null) {
            throw new RuntimeException(
                "Incorrect programming. This is to be called only by Jackson."
            );
        }
        this.type = type;
    }

    @JsonIgnore
    public static LHVarType getTypeEnumFromClass(
        Class<? extends Object> cls
    ) throws VarSubError {

        if (cls == String.class) return LHVarType.STRING;
        if (cls == Boolean.class) return LHVarType.BOOLEAN;
        if (cls == Integer.class) return LHVarType.INT;
        if (Double.class.isAssignableFrom(cls)) {
            return LHVarType.DOUBLE;
        }
        if (Map.class.isAssignableFrom(cls)) {
            return LHVarType.OBJECT;
        }
        if (List.class.isAssignableFrom(cls)) {
            return LHVarType.ARRAY;
        }

        throw new VarSubError(
            null,
            "Was unable to recognize type for " + cls.getCanonicalName() +
            ". Perhaps one of the scheduler instances is out of date."
        );
    }

    @JsonIgnore
    public Class<? extends Object> getValueClass() throws VarSubError {
        return VariableValue.getValueClass(type);
    }

    @JsonIgnore
    public static Class<? extends Object> getValueClass(LHVarType type)
    throws VarSubError {
        switch (type) {
            case INT: return Integer.class;
            case DOUBLE: return Float.class;
            case ARRAY: return List.class;
            case OBJECT: return Map.class;
            case BOOLEAN: return Boolean.class;
            case STRING: return String.class;
            default: throw new VarSubError(
                null,
                "Was unable to read serialized Variable Value. This likely means that"
                + " data was pushed to the system with a new schema and there are "
                + "still scheduler instances running with the old schema: "
                + "did not recognize the type enum."
            );
        }
    }

    @JsonIgnore
    public Object getValue() throws VarSubError {
        try {
            return LHUtil.getObjectMapper().readValue(
                serializedVal, getValueClass()
            );
        } catch(IOException exn) {
            throw new VarSubError(
                exn,
                "Was unable to read serialized Variable Value. This likely means that"
                + " data was pushed to the system with a new schema and there are "
                + "still scheduler instances running with the old schema: "
                + exn.getMessage()
            );
        }
    }

    @JsonIgnore
    public void setValue(Object val) throws VarSubError {
        if (val == null) {
            serializedVal = null;
            return;
        }

        if (!getValueClass().isAssignableFrom(val.getClass())) {
            throw new VarSubError(
                null,
                "Needed a " + getValueClass().getSimpleName() + " but got a " +
                val.getClass().getSimpleName()
            );
        }

        try {
            serializedVal = LHUtil.getObjectMapper().writeValueAsBytes(val);
        } catch(JsonProcessingException exn) {
            throw new VarSubError(
                exn, "Failed saving variable value: " + exn.getMessage()
            );
        }
    }

    @JsonIgnore
    public VariableValue castToType(LHVarType newType) throws VarSubError {
        return new VariableValue(getValueAs(newType));
    }

    @JsonIgnore
    public VariableValue castToType(Class<? extends Object> cls) throws VarSubError {
        return castToType(getTypeEnumFromClass(cls));
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public <T extends Object> T getValueAs(Class<T> cls) throws VarSubError {
        Object out = getValueAs(getTypeEnumFromClass(cls));

        try {
            return (T)out;
        } catch(Exception exn) {
            throw new VarSubError(exn, "Failed casting to " + cls.getName() +": " + exn.getMessage());
        }
    }

    @JsonIgnore
    public Object getValueAs(LHVarType newType) throws VarSubError {
        switch (newType) {
        case INT:
            return asInt();
        case DOUBLE:
            return asDouble();
        case BOOLEAN:
            return asBool();
        case STRING:
            return asString();
        case OBJECT:
            return asMap();
        case ARRAY:
            return asArray();
        default:
            throw new VarSubError(null, "Unrecognized type " + newType);
        }
    }

    @JsonIgnore
    public Integer asInt() throws VarSubError {
        Object o = getValue();
        if (o instanceof Double) {
            return ((Double)o).intValue();
        } else if (o instanceof String) {
            return Integer.valueOf((String) o);
        } else if (o instanceof Integer) {
            return (Integer) o;
        }
        throw new VarSubError(null, "Unable to convert type " + type + " to INT.");
    }

    @JsonIgnore
    public Boolean asBool() throws VarSubError {
        Object o = getValue();
        if (o == null) return false;
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else if (o instanceof String) {
            return Boolean.valueOf((String) o);
        }
        try {
            return Boolean.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubError(exn, "Failed casting " + type + " to BOOLEAN: " + exn.getMessage());
        }
    }

    @JsonIgnore
    public String asString() throws VarSubError {
        Object o = getValue();
        if (o == null) return null;
        return LHUtil.objToJsonString(o);
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<Object> asArray() throws VarSubError {
        Object o = getValue();
        if (o == null) return null;

        if (List.class.isAssignableFrom(o.getClass())) {
            return (List<Object>) o;
        } else if (o instanceof String) {
            try {
                return LHUtil.getObjectMapper().readValue(
                    (String) o, List.class
                );
            } catch (JsonProcessingException exn) {
                throw new VarSubError(exn, "Failed to convert string to list!");
            }
        }
        try {
            return List.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubError(exn, "Failed converting " + type + " to ARRAY: " + exn.getMessage());
        }
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public Map<Object, Object> asMap() throws VarSubError {
        Object o = getValue();
        if (o == null) return null;

        if (Map.class.isAssignableFrom(o.getClass())) {
            return (Map<Object, Object>) o;
        } else if (o instanceof String) {
            try {
                return LHUtil.getObjectMapper().readValue(
                    (String) o, Map.class
                );
            } catch (JsonProcessingException exn) {
                throw new VarSubError(exn, "Failed to convert string to list!");
            }
        }
        try {
            return Map.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubError(exn, "Failed converting " + type + " to OBJECT: " + exn.getMessage());
        }
    }

    @JsonIgnore
    public Double asDouble() throws VarSubError {
        Object o = getValue();
        if (o instanceof Double) {
            return (Double) o;
        } else if (o instanceof String) {
            return Double.valueOf((String) o);
        } else if (o instanceof Integer) {
            return Double.valueOf((Integer) o);
        }
        try {
            return Double.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubError(exn, "Failed converting " + type + " to DOUBLE: " + exn.getMessage());
        }
    }

    // Methods below are used for variable mutations.
    public boolean isInTypes(LHVarType...types) {
        for (LHVarType t : types) {
            if (t == type) return true;
        }
        return false;
    }

    public VariableValue add(VariableValue other) throws VarSubError {
        if (isInTypes(LHVarType.BOOLEAN, LHVarType.OBJECT) ||
            other.isInTypes(LHVarType.BOOLEAN, LHVarType.OBJECT)) {

            throw new VarSubError(null, "Tried to add a boolean or object. Not allowed!");
        }

        if (type == LHVarType.INT) {
            return new VariableValue(asInt() + other.asInt());

        } else if (type == LHVarType.STRING) {
            return new VariableValue(asString() + other.asString());

        } else if (type == LHVarType.ARRAY) {
            List<Object> out = asArray();
            out.add(other.getValue());
            return new VariableValue(out);

        } else {
            assert type == LHVarType.DOUBLE;
            return new VariableValue(asDouble() + other.asDouble());
        }
    }

    public VariableValue divide(VariableValue other) throws VarSubError {
        Double o = other.asDouble();
        if (o == 0.0 || o == null) {
            throw new VarSubError(null, "Tried to divide by zero or null.");
        }

        VariableValue newVal = new VariableValue(asDouble() / o);
        return newVal.castToType(type);
    }

    public VariableValue multiply(VariableValue other) throws VarSubError {
        Double o = other.asDouble();
        if (o == null) {
            throw new VarSubError(null, "Tried to multiply by null.");
        }
        return new VariableValue(asDouble() * o).castToType(type);
    }

    public VariableValue subtract(VariableValue other) throws VarSubError {
        Double o = other.asDouble();
        if (o == null) {
            throw new VarSubError(null, "Tried to subtract null.");
        }
        return new VariableValue(asDouble() - o).castToType(type);
    }

    public VariableValue extend(VariableValue other) throws VarSubError {
        if (other.getType() != LHVarType.ARRAY || type != LHVarType.ARRAY) {
            throw new VarSubError(null, "Can only extend two arrays!");
        }

        List<Object> lhsList = asArray();
        for (Object o: other.asArray()) {
            lhsList.add(o);
        }
        return new VariableValue(lhsList);
    }

    public VariableValue removeIfPresent(VariableValue other) throws VarSubError {
        List<Object> lhsList = asArray();
        Object o = other.getValue();
        lhsList.removeIf((i) -> Objects.equals(i, o));
        return new VariableValue(lhsList);
    }

    public VariableValue removeIndex(VariableValue other) throws VarSubError {
        List<Object> lhsList = asArray();
        Integer idx = other.asInt();
        lhsList.remove(idx);
        return new VariableValue(lhsList);
    }

    public VariableValue removeKey(VariableValue other) throws VarSubError {
        Map<Object, Object> lhsMap = asMap();
        lhsMap.remove(other.getValue());
        return new VariableValue(lhsMap);
    }

    public VariableValue copy() {
        VariableValue val = new VariableValue();
        try {
            val.setSerializedVal(serializedVal);
            val.setType(type);
        } catch(VarSubError exn) {
            // This shouldn't be possible
            throw new RuntimeException(exn);
        }
        return val;
    }

    public Integer compare(VariableValue o) throws VarSubError {
        switch (type) {
            case STRING:
                return asString().compareTo(o.asString());
            case INT:
                return asInt().compareTo(o.asInt());
            case DOUBLE:
                return asDouble().compareTo(o.asDouble());
            case BOOLEAN:
                asBool().compareTo(o.asBool());
            default:
                throw new VarSubError(null, "Tried to compare an invalid type: " + type);
        }
    }

    public boolean contains(VariableValue other) throws VarSubError {
        if (!isInTypes(LHVarType.STRING, LHVarType.OBJECT, LHVarType.ARRAY)) {
            throw new VarSubError(
                null, "Tried to check if a " + type + " contains another object!"
            );
        } 

        if (type == LHVarType.STRING) {
            return asString().contains(other.asString());
        }

        if (type == LHVarType.OBJECT) {
            return asMap().containsKey(other.asString());
        }

        assert type == LHVarType.ARRAY;
        return asArray().contains(other.getValue());
    }
}
