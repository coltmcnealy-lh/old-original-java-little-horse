package little.horse.common.objects.rundata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import little.horse.common.DepInjContext;
import little.horse.common.objects.metadata.VariableMutationOperation;
import little.horse.common.objects.metadata.WFRunVariableDef;
import little.horse.common.objects.metadata.WFRunVariableTypeEnum;

import little.horse.common.util.LHUtil;

public class Mutation {
    public Object lhs;
    public Object rhs;
    public VariableMutationOperation op;
    public ThreadRun tr;
    public WFRunVariableDef varDef;
    public String varName;
    public DepInjContext config;

    public Mutation(
        Object lhs, Object rhs, VariableMutationOperation op, ThreadRun tr,
        WFRunVariableDef varDef, String varName, DepInjContext config
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
                ", lhs: " + LHUtil.stringify(lhs) + ", rhs: " + 
                LHUtil.stringify(rhs) + ":\n" + exn.getMessage()
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

        if (!dryRun) tr.variables.put(varName, newOut);
    }

    public static boolean isInTypes(
        WFRunVariableTypeEnum needle,
        WFRunVariableTypeEnum... haystack
    ) {
        for (WFRunVariableTypeEnum hay : haystack) {
            if (needle == hay) return true;
        }
        return false;
    }

    private Object handleAssign() throws VarSubOrzDash {
        Object out = rhs;
        Class<?> defTypeCls = LHUtil.getNeededClass(varDef);
        if (out != null && !defTypeCls.isInstance(out)) {
            throw new VarSubOrzDash(null,
                "Tried to set var " + varName + ", which is of type " +
                defTypeCls.getName() + " to " + out.toString() + ", which is " +
                " of type " + out.getClass().getName()
            );
        }

        if (defTypeCls == Object.class) {
            // Then the thing needs to either be a Map or be loadable.
            if (String.class.isInstance(out)) {
                out = LHUtil.unjsonify((String) out, config);
            }
        }

        return out;
    }

    private Object handleAdd() throws VarSubOrzDash {
        if (varDef.type == WFRunVariableTypeEnum.BOOLEAN ||
            varDef.type == WFRunVariableTypeEnum.OBJECT
        ) {
            throw new VarSubOrzDash(
                null,
                "had an invalid wfspec. Tried to add a boolean or object."
            );
        }

        // Just try to cast the right hand side to what it's supposed to be
        // in order to verify that it'll work.
        if (varDef.type == WFRunVariableTypeEnum.INT) {
            return (Integer) rhs + (Integer) lhs;

        } else if (varDef.type == WFRunVariableTypeEnum.STRING) {
            return (String) lhs + (String) rhs;

        } else if (varDef.type == WFRunVariableTypeEnum.ARRAY) {
            // nothing to verify here until we start enforcing json schemas
            // within arrays
            @SuppressWarnings("unchecked")
            ArrayList<Object> lhsArr = (ArrayList<Object>) lhs;
            lhsArr.add(rhs);
            return lhsArr;

        } else {
            assert (varDef.type == WFRunVariableTypeEnum.FLOAT);
            return (Float) lhs + (Float) rhs;
        }
    }

    private Object handleExtend() throws VarSubOrzDash {
        if (varDef.type != WFRunVariableTypeEnum.ARRAY ||
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
        checkAreArithmeticable();
        Float lfloat = (Float) lhs;
        Float rfloat = (Float) rhs;

        if (rfloat == 0) {
            throw new VarSubOrzDash(null, "tried to DIVIDE by zero!");
        }

        Float out = lfloat / rfloat;
        return (varDef.type == WFRunVariableTypeEnum.FLOAT) ? out : out.intValue();
    }

    private Object handleSubtract() throws VarSubOrzDash {
        checkAreArithmeticable();
        Float lfloat = (Float) lhs;
        Float rfloat = (Float) rhs;

        Float out = lfloat - rfloat;
        return (varDef.type == WFRunVariableTypeEnum.FLOAT) ? out : out.intValue();
    }

    private Object handleMultiply() throws VarSubOrzDash {
        checkAreArithmeticable();
        Float lfloat = (Float) lhs;
        Float rfloat = (Float) rhs;

        Float out = lfloat * rfloat;
        return (varDef.type == WFRunVariableTypeEnum.FLOAT) ? out : out.intValue();
    
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
    public static boolean contains(Object left, Object right) throws VarSubOrzDash {
        try {
            Collection<Object> collection = (Collection<Object>) left;
            for (Object thing : collection) {
                if (thing.equals(right)) {
                    return true;
                }
            }
        } catch (Exception exn) {
            exn.printStackTrace();
            throw new VarSubOrzDash(
                exn,
                "Failed determing whether the left contains the right: " +
                LHUtil.stringify(left) + " , " + LHUtil.stringify(right)
            );
        }
        return false;

    }

    @SuppressWarnings("all") // lol
    public static int compare(Object left, Object right) throws VarSubOrzDash {

        try {
            int result = ((Comparable) left).compareTo((Comparable) right);
            return result;
        } catch(Exception exn) {
            LHUtil.logError(exn.getMessage());
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }

    private void checkAreArithmeticable() throws VarSubOrzDash {
        if (varDef.type != WFRunVariableTypeEnum.INT &&
            varDef.type != WFRunVariableTypeEnum.FLOAT
        ) {
            throw new VarSubOrzDash(
                null,
                "LHS for DIVIDE needs to be INT or FLOAT, but got" +
                varDef.type
            );
        }

        if (lhs == null) {
            throw new VarSubOrzDash(null, "tried to DIVIDE with null lhs");
        }

        if (rhs == null) {
            throw new VarSubOrzDash(null, "tried to DIVIDE with null rhs");
        }

        if (!(rhs instanceof Integer) && !(rhs instanceof Float)) {
            throw new VarSubOrzDash(
                null,
                "RHS for divide needs to be INT or FLOAT but got " +
                rhs.getClass().getCanonicalName()
            );
        }

    }
}
