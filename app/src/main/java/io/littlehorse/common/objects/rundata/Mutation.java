package io.littlehorse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.objects.metadata.VariableMutationOperation;
import io.littlehorse.common.objects.metadata.VariableValue;
import io.littlehorse.common.objects.metadata.WFRunVariableDef;
import io.littlehorse.common.objects.metadata.LHVarType;
import io.littlehorse.common.util.LHUtil;

public class Mutation {
    public Object lhs;
    public Object rhs;
    public VariableMutationOperation op;
    public ThreadRun tr;
    public WFRunVariableDef varDef;
    public String varName;
    public LHConfig config;

    public Mutation(
        Object lhs, Object rhs, VariableMutationOperation op, ThreadRun tr,
        WFRunVariableDef varDef, String varName, LHConfig config
    ) {
        this.lhs = LHUtil.lhCopy(lhs);
        this.rhs = LHUtil.lhCopy(rhs);
        this.op = op;
        this.tr = tr;
        this.varDef = varDef;
        this.varName = varName;
        this.config = config;
    }

    public void execute(boolean dryRun) throws VarSubOrzDash {
        try {
            doExecuteHelper(dryRun);
        } catch (VarSubOrzDash vsod) {
            throw vsod;
        } catch (Exception exn) {
            exn.printStackTrace();
            throw new VarSubOrzDash(
                exn,
                "Had an unexpected error mutating variable " + varName +
                ", lhs: " + LHUtil.objToJsonString(lhs) + ", rhs: " + 
                LHUtil.objToJsonString(rhs) + ":\n" + exn.getMessage()
            );
        }
    }

    private void doExecuteHelper(boolean dryRun) throws VarSubOrzDash {
        // Can't rely upon the WFRunVariableDefSchema because if, for example, the
        // LHS variable is an OBJECT, and there is a jsonpath, we could index from
        // the object to an Integer, in which case the LHS is not of the same type
        // as the varDef.type; but we will get more fancy once we add jsonschema
        // validation.

        Object newOut = null;

        // Now we handle every operation that's legal. Because I'm lazy, there's
        // only two so far.
        if (op == VariableMutationOperation.ASSIGN) {
            newOut = handleAssign();

        } else if (op == VariableMutationOperation.ADD) {
            newOut = handleAdd();

        } else if (op == VariableMutationOperation.EXTEND) {
            newOut = handleExtend();

        } else if (op == VariableMutationOperation.DIVIDE) {
            newOut = handleDivide();

        } else if (op == VariableMutationOperation.SUBTRACT) {
            newOut = handleSubtract();

        } else if (op == VariableMutationOperation.MULTIPLY) {
            newOut = handleMultiply();

        } else if (op == VariableMutationOperation.REMOVE_IF_PRESENT) {
            newOut = handleRemoveIfPresent();

        } else if (op == VariableMutationOperation.REMOVE_KEY) {
            newOut = handleRemoveKey();

        } else if (op == VariableMutationOperation.REMOVE_INDEX) {
            newOut = handleRemoveIndex();

        } else {
            throw new VarSubOrzDash(
                null,
                "Got an invalid variable mutation operation: " + op
            );
        }

        newOut = newOut == null ? null : coerceBackToType(newOut);

        if (!dryRun) {
            tr.variables.put(varName, newOut);
        }
    }

    private Object coerceBackToType(Object o) throws VarSubOrzDash {
        // Class<?> defTypeCls = LHUtil.getNeededClass(varDef);
        if (varDef.type == LHVarType.INT) {
            return toInt(o);
        } else if (varDef.type == LHVarType.ARRAY) {
            return toArray(o);
        } else if (varDef.type == LHVarType.OBJECT) {
            return toMap(o);
        } else if (varDef.type == LHVarType.DOUBLE) {
            return toDouble(o);
        } else if (varDef.type == LHVarType.STRING) {
            return toStr(o);
        } else if (varDef.type == LHVarType.BOOLEAN) {
            return toBool(o);
        } else {
            throw new VarSubOrzDash(null, "Impossible to get here");
        }
    }

    private Integer toInt(Object o) {
        if (o instanceof Double) {
            return ((Double)o).intValue();
        } else if (o instanceof String) {
            return Integer.valueOf((String) o);
        } else if (o instanceof Integer) {
            return (Integer) o;
        }
        return Integer.class.cast(o);
    }

    private Boolean toBool(Object o) {
        if (o instanceof Boolean) {
            return (Boolean) o;
        } else if (o instanceof String) {
            return Boolean.valueOf((String) o);
        }
        return Boolean.class.cast(o);
    }

    private String toStr(Object o) {
        if (o == null) return "";
        // Someday this might get more sophisticated...
        return String.valueOf(o);
    }

    @SuppressWarnings("unchecked")
    private List<Object> toArray(Object o) throws VarSubOrzDash {
        if (o instanceof List) {
            return (List<Object>) o;
        } else if (o instanceof String) {
            try {
                return LHUtil.getObjectMapper(config).readValue(
                    (String) o, List.class
                );
            } catch (JsonProcessingException exn) {
                throw new VarSubOrzDash(exn, "Failed to convert string to list!");
            }
        } else {
            throw new VarSubOrzDash(null, "Unable to cast type " + type + " to ARRAY");
        }
        return List.class.cast(o);
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Object> toMap(Object o) throws VarSubOrzDash {
        if (o instanceof Map) {
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
        return Map.class.cast(o);
    }

    private Object handleAssign() throws VarSubOrzDash {
        return rhs;

    }

    private Double toDouble(Object o) {
        if (o instanceof Double) {
            return (Double) o;
        } else if (o instanceof String) {
            return Double.valueOf((String) o);
        } else if (o instanceof Integer) {
            return Double.valueOf((Integer) o);
        }
        return Double.class.cast(o);
    }

    private Object handleAdd() throws VarSubOrzDash {
        if (varDef.type == LHVarType.BOOLEAN ||
            varDef.type == LHVarType.OBJECT
        ) {
            throw new VarSubOrzDash(
                null,
                "had an invalid wfspec. Tried to add a boolean or object."
            );
        }

        // Just try to cast the right hand side to what it's supposed to be
        // in order to verify that it'll work.
        if (varDef.type == LHVarType.INT) {
            return toInt(rhs) + toInt(lhs);

        } else if (varDef.type == LHVarType.STRING) {
            return toStr(lhs) + toStr(rhs);

        } else if (varDef.type == LHVarType.ARRAY) {
            // nothing to verify here until we start enforcing json schemas
            // within arrays
            @SuppressWarnings("unchecked")
            ArrayList<Object> lhsArr = (ArrayList<Object>) lhs;
            lhsArr.add(rhs);
            return lhsArr;

        } else {
            assert (varDef.type == LHVarType.DOUBLE);
            return toDouble(lhs) + toDouble(rhs);
        }
    }

    private Object handleExtend() throws VarSubOrzDash {
        if (varDef.type != LHVarType.ARRAY ||
            !(rhs instanceof List)
        ) {
            throw new VarSubOrzDash(null,
                "Can only EXTEND two array's."
            );
        }

        @SuppressWarnings("unchecked")
        List<Object> lhsArr = (List<Object>) lhs;

        @SuppressWarnings("unchecked")
        List<Object> rlist = (List<Object>) rhs;
        for (Object o : rlist) {
            lhsArr.add(o);
        }
        return lhsArr;
    }

    private Object handleDivide() throws VarSubOrzDash {
        Double lfloat = toDouble(lhs);
        Double rfloat = toDouble(rhs);

        if (rfloat == 0) {
            throw new VarSubOrzDash(null, "tried to DIVIDE by zero!");
        }

        Double out = lfloat / rfloat;
        return (varDef.type == LHVarType.DOUBLE) ? out : out.intValue();
    }

    private Object handleSubtract() throws VarSubOrzDash {
        Double lfloat = toDouble(lhs);
        Double rfloat = toDouble(rhs);
        Double out = lfloat - rfloat;
        return (varDef.type == LHVarType.DOUBLE) ? out : out.intValue();
    }

    private Object handleMultiply() throws VarSubOrzDash {
        Double lfloat = toDouble(lhs);
        Double rfloat = toDouble(rhs);

        Double out = lfloat * rfloat;
        return (varDef.type == LHVarType.DOUBLE) ? out : out.intValue();
    
    }

    private Object handleRemoveIfPresent() throws VarSubOrzDash {
        @SuppressWarnings("unchecked")
        List<Object> lhsList = List.class.cast(lhs);
        lhsList.removeIf((i) -> Objects.equals(i, rhs));
        return lhsList;
    }

    private Object handleRemoveKey() throws VarSubOrzDash {
        @SuppressWarnings("unchecked")
        Map<Object, Object> lhsMap = Map.class.cast(lhs);
        lhsMap.remove(rhs);
        return lhsMap;
    }

    private Object handleRemoveIndex() throws VarSubOrzDash {
        @SuppressWarnings("unchecked")
        List<Object> lhsList = List.class.cast(lhs);
        lhsList.remove(rhs);
        return lhsList;
    }

    /**
     * Used to evaluate Workflow Conditional Branching Expressions. The Left is an
     * object of some sort, and the right is another. Returns true if the Left
     * has the Right inside it.
     * @param left haystack
     * @param right needle
     * @return true if haystack has the needle in it.
     * @throws VarSubOrzDash if we can't cast the left to a container of objects,
     * or if we get an exception while comparing the equality of two things inside
     * right.
     */
    @SuppressWarnings("all")
    public static boolean contains(VariableValue left, VariableValue right)
    throws VarSubOrzDash {
        try {
            Collection<Object> collection = (Collection<Object>) left.getValue();
            for (Object thing : collection) {
                if (thing.equals(right.getValue())) {
                    return true;
                }
            }
        } catch (Exception exn) {
            exn.printStackTrace();
            throw new VarSubOrzDash(
                exn,
                "Failed determing whether the left contains the right: " +
                LHUtil.objToJsonString(left.getValue()) + " , " +
                LHUtil.objToJsonString(right.getValue())
            );
        }
        return false;

    }

    @SuppressWarnings("all") // lol
    public static int compare(VariableValue left, VariableValue right)
    throws VarSubOrzDash {
        try {
            return ((Comparable) left.getValue()).compareTo(
                (Comparable) right.getValue());
        } catch(Exception exn) {
            LHUtil.logError(exn.getMessage());
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }
}
