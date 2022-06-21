package io.littlehorse.common.objects.metadata;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.objects.rundata.VarSubOrzDash;
import io.littlehorse.common.util.LHUtil;

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

    public VariableValue(LHConfig config) {
        this.config = config;
    }

    public VariableValue(LHConfig config, Object val) throws VarSubOrzDash {
        this.config = config;

        if (val != null) {
            type = getTypeEnumFromClass(val.getClass());
            this.setValue(val);
        }
    }

    public byte[] getSerializedVal() {
        return serializedVal;
    }

    public void setSerializedVal(byte[] newVal) throws VarSubOrzDash {
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
    ) throws VarSubOrzDash {

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

        throw new VarSubOrzDash(
            null,
            "Was unable to recognize type for " + cls.getCanonicalName() +
            ". Perhaps one of the scheduler instances is out of date."
        );
    }

    @JsonIgnore
    public Class<? extends Object> getValueClass() throws VarSubOrzDash {
        return VariableValue.getValueClass(type);
    }

    @JsonIgnore
    public static Class<? extends Object> getValueClass(LHVarType type)
    throws VarSubOrzDash {
        switch (type) {
            case INT: return Integer.class;
            case DOUBLE: return Float.class;
            case ARRAY: return List.class;
            case OBJECT: return Map.class;
            case BOOLEAN: return Boolean.class;
            case STRING: return String.class;
            default: throw new VarSubOrzDash(
                null,
                "Was unable to read serialized Variable Value. This likely means that"
                + " data was pushed to the system with a new schema and there are "
                + "still scheduler instances running with the old schema: "
                + "did not recognize the type enum."
            );
        }
    }

    @JsonIgnore
    public Object getValue() throws VarSubOrzDash {
        try {
            return LHUtil.getObjectMapper(config).readValue(
                serializedVal, getValueClass()
            );
        } catch(IOException exn) {
            throw new VarSubOrzDash(
                exn,
                "Was unable to read serialized Variable Value. This likely means that"
                + " data was pushed to the system with a new schema and there are "
                + "still scheduler instances running with the old schema: "
                + exn.getMessage()
            );
        }
    }

    @JsonIgnore
    public void setValue(Object val) throws VarSubOrzDash {
        if (val == null) {
            serializedVal = null;
            return;
        }

        if (!getValueClass().isAssignableFrom(val.getClass())) {
            throw new VarSubOrzDash(
                null,
                "Needed a " + getValueClass().getSimpleName() + " but got a " +
                val.getClass().getSimpleName()
            );
        }

        try {
            serializedVal = LHUtil.getObjectMapper(config).writeValueAsBytes(val);
        } catch(JsonProcessingException exn) {
            throw new VarSubOrzDash(
                exn, "Failed saving variable value: " + exn.getMessage()
            );
        }
    }

    @JsonIgnore
    public VariableValue castToType(LHVarType newType) throws VarSubOrzDash {
        return new VariableValue(config, getValueAs(newType));
    }

    @JsonIgnore
    public VariableValue castToType(Class<? extends Object> cls) throws VarSubOrzDash {
        return castToType(getTypeEnumFromClass(cls));
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public <T extends Object> T getValueAs(Class<T> cls) throws VarSubOrzDash {
        Object out = getValueAs(getTypeEnumFromClass(cls));

        try {
            return (T)out;
        } catch(Exception exn) {
            throw new VarSubOrzDash(exn, "Failed casting to " + cls.getName() +": " + exn.getMessage());
        }
    }

    @JsonIgnore
    public Object getValueAs(LHVarType newType) throws VarSubOrzDash {
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
            throw new VarSubOrzDash(null, "Unrecognized type " + newType);
        }
    }

    @JsonIgnore
    public Integer asInt() throws VarSubOrzDash {
        Object o = getValue();
        if (o instanceof Double) {
            return ((Double)o).intValue();
        } else if (o instanceof String) {
            return Integer.valueOf((String) o);
        } else if (o instanceof Integer) {
            return (Integer) o;
        }
        throw new VarSubOrzDash(null, "Unable to convert type " + type + " to INT.");
    }

    @JsonIgnore
    public Boolean asBool() throws VarSubOrzDash {
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
            throw new VarSubOrzDash(exn, "Failed casting " + type + " to BOOLEAN: " + exn.getMessage());
        }
    }

    @JsonIgnore
    public String asString() throws VarSubOrzDash {
        Object o = getValue();
        if (o == null) return null;
        return LHUtil.objToJsonString(o);
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public List<Object> asArray() throws VarSubOrzDash {
        Object o = getValue();
        if (o == null) return null;

        if (List.class.isAssignableFrom(o.getClass())) {
            return (List<Object>) o;
        } else if (o instanceof String) {
            try {
                return LHUtil.getObjectMapper(config).readValue(
                    (String) o, List.class
                );
            } catch (JsonProcessingException exn) {
                throw new VarSubOrzDash(exn, "Failed to convert string to list!");
            }
        }
        try {
            return List.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubOrzDash(exn, "Failed converting " + type + " to ARRAY: " + exn.getMessage());
        }
    }

    @JsonIgnore
    @SuppressWarnings("unchecked")
    public Map<Object, Object> asMap() throws VarSubOrzDash {
        Object o = getValue();
        if (o == null) return null;

        if (Map.class.isAssignableFrom(o.getClass())) {
            return (Map<Object, Object>) o;
        } else if (o instanceof String) {
            try {
                return LHUtil.getObjectMapper(config).readValue(
                    (String) o, Map.class
                );
            } catch (JsonProcessingException exn) {
                throw new VarSubOrzDash(exn, "Failed to convert string to list!");
            }
        }
        try {
            return Map.class.cast(o);
        } catch(ClassCastException exn) {
            throw new VarSubOrzDash(exn, "Failed converting " + type + " to OBJECT: " + exn.getMessage());
        }
    }

    @JsonIgnore
    public Double asDouble() throws VarSubOrzDash {
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
            throw new VarSubOrzDash(exn, "Failed converting " + type + " to DOUBLE: " + exn.getMessage());
        }
    }

    // Methods below are used for variable mutations.
    public boolean isInTypes(LHVarType...types) {
        for (LHVarType t : types) {
            if (t == type) return true;
        }
        return false;
    }

    public VariableValue add(VariableValue other) throws VarSubOrzDash {
        if (isInTypes(LHVarType.BOOLEAN, LHVarType.OBJECT) ||
            other.isInTypes(LHVarType.BOOLEAN, LHVarType.OBJECT)) {

            throw new VarSubOrzDash(null, "Tried to add a boolean or object. Not allowed!");
        }

        if (type == LHVarType.INT) {
            return new VariableValue(config, asInt() + other.asInt());

        } else if (type == LHVarType.STRING) {
            return new VariableValue(config, asString() + other.asString());

        } else if (type == LHVarType.ARRAY) {
            List<Object> out = asArray();
            out.add(other.getValue());
            return new VariableValue(config, out);

        } else {
            assert type == LHVarType.DOUBLE;
            return new VariableValue(config, asDouble() + other.asDouble());
        }
    }

    public VariableValue divide(VariableValue other) throws VarSubOrzDash {
        Double o = other.asDouble();
        if (o == 0.0 || o == null) {
            throw new VarSubOrzDash(null, "Tried to divide by zero or null.");
        }

        VariableValue newVal = new VariableValue(config, asDouble() / o);
        return newVal.castToType(type);
    }

    public VariableValue multiply(VariableValue other) throws VarSubOrzDash {
        Double o = other.asDouble();
        if (o == null) {
            throw new VarSubOrzDash(null, "Tried to multiply by null.");
        }
        return new VariableValue(config, asDouble() * o).castToType(type);
    }

    public VariableValue subtract(VariableValue other) throws VarSubOrzDash {
        Double o = other.asDouble();
        if (o == null) {
            throw new VarSubOrzDash(null, "Tried to subtract null.");
        }
        return new VariableValue(config, asDouble() - o).castToType(type);
    }

    public VariableValue extend(VariableValue other) throws VarSubOrzDash {
        if (other.getType() != LHVarType.ARRAY || type != LHVarType.ARRAY) {
            throw new VarSubOrzDash(null, "Can only extend two arrays!");
        }

        List<Object> lhsList = asArray();
        for (Object o: other.asArray()) {
            lhsList.add(o);
        }
        return new VariableValue(config, lhsList);
    }

    public VariableValue removeIfPresent(VariableValue other) throws VarSubOrzDash {
        List<Object> lhsList = asArray();
        Object o = other.getValue();
        lhsList.removeIf((i) -> Objects.equals(i, o));
        return new VariableValue(config, lhsList);
    }

    public VariableValue removeIndex(VariableValue other) throws VarSubOrzDash {
        List<Object> lhsList = asArray();
        Integer idx = other.asInt();
        lhsList.remove(idx);
        return new VariableValue(config, lhsList);
    }

    public VariableValue removeKey(VariableValue other) throws VarSubOrzDash {
        Map<Object, Object> lhsMap = asMap();
        lhsMap.remove(other.getValue());
        return new VariableValue(config, lhsMap);
    }

    public VariableValue copy() {
        VariableValue val = new VariableValue(config);
        try {
            val.setSerializedVal(serializedVal);
            val.setType(type);
        } catch(VarSubOrzDash exn) {
            // This shouldn't be possible
            throw new RuntimeException(exn);
        }
        return val;
    }

    public Integer compare(VariableValue o) throws VarSubOrzDash {
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
                throw new VarSubOrzDash(null, "Tried to compare an invalid type: " + type);
        }
    }

    public boolean contains(VariableValue other) throws VarSubOrzDash {
        if (!isInTypes(LHVarType.STRING, LHVarType.OBJECT, LHVarType.ARRAY)) {
            throw new VarSubOrzDash(
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
