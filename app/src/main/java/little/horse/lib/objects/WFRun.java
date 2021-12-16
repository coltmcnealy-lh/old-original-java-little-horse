package little.horse.lib.objects;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import com.jayway.jsonpath.JsonPath;

import little.horse.lib.Config;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHStatus;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.VarSubOrzDash;
import little.horse.lib.schemas.EdgeConditionSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.VariableDefinitionSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFRunVariableContexSchema;

public class WFRun {
    private WFRunSchema schema;
    private WFSpec wfSpec;
    private Config config;

    private void processSchema() {
        if (schema.guid == null) {
            schema.guid = LHUtil.generateGuid();
        }

        if (schema.wfSpecGuid == null) {
            schema.wfSpecGuid = wfSpec.getModel().guid;
        }
        if (schema.wfSpecName == null) {
            schema.wfSpecGuid = wfSpec.getModel().name;
        }

        if (schema.status == null) {
            schema.status = LHStatus.PENDING;
        }
    }

    public static WFRunVariableContexSchema getContext(WFRunSchema wfRun) {
        WFRunVariableContexSchema schema = new WFRunVariableContexSchema();

        schema.taskRuns = new HashMap<String, ArrayList<TaskRunSchema>>();
        for (TaskRunSchema tr : wfRun.taskRuns) {
            if (!schema.taskRuns.containsKey(tr.nodeName)) {
                schema.taskRuns.put(tr.nodeName, new ArrayList<TaskRunSchema>());
            }
            schema.taskRuns.get(tr.nodeName).add(tr);
        }

        schema.wfRunVariables = wfRun.variables;
        return schema;
    }

    public static String getContextString(WFRunSchema wfRun) {
        WFRunVariableContexSchema schema = WFRun.getContext(wfRun);
        return schema.toString();
    }

    public WFRun(WFRunSchema schema, Config config) throws LHLookupException, LHValidationError {
        this.config = config;
        this.schema = schema;
        this.wfSpec = getWFSpec();
        this.processSchema();
    }

    public WFSpec getWFSpec() throws LHLookupException, LHValidationError {
        if (schema.wfSpecGuid != null) {
            return WFSpec.fromIdentifier(schema.wfSpecGuid, config);
        } else if (schema.wfSpecName != null) {
            return WFSpec.fromIdentifier(schema.wfSpecName, config);
        }
        throw new LHValidationError(
            "Did not provide wfSpecName nor Guid for wfRun " + this.schema.guid
        );
    }

    public WFRun(WFRunSchema schema, Config config, WFSpec wfSpec) {
        this.config = config;
        this.schema = schema;
        this.wfSpec = wfSpec;
        this.processSchema();
    }

    public WFRunSchema getModel() {
        return this.schema;
    }

    public String toString() {
        return schema.toString();
    }

    public static boolean evaluateEdge(
        WFRunSchema wfRun, EdgeConditionSchema condition
    ) throws VarSubOrzDash {
        if (condition == null) return true;
        Object lhs = getVariableSubstitution(wfRun, condition.leftSide);
        Object rhs = getVariableSubstitution(wfRun, condition.rightSide);
        switch (condition.comparator) {
            case LESS_THAN: return compare(lhs, rhs) < 0;
            case LESS_THAN_EQ: return compare(lhs, rhs) <= 0;
            case GREATER_THAN: return compare(lhs, rhs) > 0;
            case GRREATER_THAN_EQ: return compare(lhs, rhs) >= 0;
            case EQUALS: return compare(lhs, rhs) == 0;
            case NOT_EQUALS: return compare(lhs, rhs) != 0;
            case IN: return contains(lhs, rhs);
            case NOT_IN: return !contains(lhs, rhs);
            default: return false;
        }
    }

    @SuppressWarnings("all")
    private static boolean contains(Object left, Object right) throws VarSubOrzDash {
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
                "Failed determing whether the left contains the right "
            );
        }
        return false;

    }

    @SuppressWarnings("all") // lol
    private static int compare(Object left, Object right) throws VarSubOrzDash {
        try {
            int result = ((Comparable) left).compareTo((Comparable) right);
            return result;
        } catch(Exception exn) {
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }

    public static Object getVariableSubstitution(
            WFRunSchema wfRun, VariableDefinitionSchema var
    ) throws VarSubOrzDash {

        WFRunVariableContexSchema context = getContext(wfRun);
        if (var.literalValue != null) {
            return var.literalValue;
        }

        // at this point, the thing must either come from a previous taskRun or
        // from a wfRunVariable.

        String dataToParse = null;
        if (var.nodeName != null) {
            ArrayList<TaskRunSchema> taskRuns = context.taskRuns.get(var.nodeName);
            if (taskRuns == null && var.defaultValue == null) {
                throw new VarSubOrzDash(
                    null,
                    "Could not find taskRuns for node name " + var.nodeName
                );
            }
            if (taskRuns == null) {
                return var.defaultValue;
            } else if (var.useLatestTaskRun) {
                dataToParse = taskRuns.get(taskRuns.size() - 1).toString();
            } else {
                dataToParse = LHUtil.jsonify(taskRuns);
            }
        } else if (var.wfRunVariableName != null) {
            HashMap<String, Object> variableContext = context.wfRunVariables;
            Object result = variableContext.get(var.wfRunVariableName);
            if (result == null) {
                throw new VarSubOrzDash(
                    null,
                    "No variable named " + var.wfRunVariableName + " in context."
                );
            }
            dataToParse = result.toString();
        }

        if (dataToParse == null) {
            // Then we need to have a literal value.
            assert (var.defaultValue != null);
            return var.defaultValue;
        }
        if (var.jsonPath == null) {
            // just return the whole thing
            return dataToParse;
        }

        try {
            return JsonPath.parse(dataToParse).read(var.jsonPath);
        } catch(Exception exn) {
            throw new VarSubOrzDash(
                exn,
                "Specified jsonpath " + var.jsonPath + " failed to resolve on " + dataToParse
            );
        }
    }
}