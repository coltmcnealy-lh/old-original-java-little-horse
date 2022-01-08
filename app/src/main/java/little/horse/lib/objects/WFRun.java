package little.horse.lib.objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jayway.jsonpath.JsonPath;

import little.horse.lib.Config;
import little.horse.lib.Constants;
import little.horse.lib.LHLookupException;
import little.horse.lib.LHLookupExceptionReason;
import little.horse.lib.LHUtil;
import little.horse.lib.LHValidationError;
import little.horse.lib.VarSubOrzDash;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.EdgeConditionSchema;
import little.horse.lib.schemas.TaskRunSchema;
import little.horse.lib.schemas.VariableAssignmentSchema;
import little.horse.lib.schemas.VariableMutationOperation;
import little.horse.lib.schemas.VariableMutationSchema;
import little.horse.lib.schemas.WFRunMetadataEnum;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFRunVariableContexSchema;
import little.horse.lib.schemas.WFRunVariableDefSchema;
import little.horse.lib.schemas.WFRunVariableTypeEnum;
import little.horse.lib.schemas.WFTokenSchema;
import little.horse.lib.wfRuntime.WFRunStatus;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

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
            schema.status = WFRunStatus.RUNNING;
        }
    }

    public static WFRunVariableContexSchema getContext(WFRunSchema wfRun, WFTokenSchema token) {
        WFRunVariableContexSchema schema = new WFRunVariableContexSchema();

        Integer tokenNumber = token == null ? null : token.tokenNumber;

        schema.nodeOutputs = new HashMap<String, ArrayList<TaskRunSchema>>();
        for (WFTokenSchema tok : wfRun.tokens) {
            if (tok.tokenNumber == tokenNumber) {
                // Skip it for now and come back later to ensure that this token is last.
                continue;
            }
            for (TaskRunSchema tr : tok.taskRuns) {
                if (!schema.nodeOutputs.containsKey(tr.nodeName)) {
                    schema.nodeOutputs.put(tr.nodeName, new ArrayList<TaskRunSchema>());
                }
                schema.nodeOutputs.get(tr.nodeName).add(tr);
            }
        }

        if (tokenNumber != null) {
            for (TaskRunSchema tr : token.taskRuns) {
                if (!schema.nodeOutputs.containsKey(tr.nodeName)) {
                    schema.nodeOutputs.put(tr.nodeName, new ArrayList<TaskRunSchema>());
                }
                schema.nodeOutputs.get(tr.nodeName).add(tr);
            }
        }

        schema.wfRunVariables = wfRun.variables;
        return schema;
    }

    public static WFRun fromGuid(String guid, Config config)
    throws LHLookupException, LHValidationError {
        OkHttpClient client = config.getHttpClient();
        String url = config.getAPIUrlFor(Constants.WF_RUN_API_PATH) + "/" + guid;
        Request request = new Request.Builder().url(url).build();
        Response response;
        String responseBody = null;

        try {
            response = client.newCall(request).execute();
            responseBody = response.body().string();
        }
        catch (IOException exn) {
            String err = "Got an error making request to " + url + ": " + exn.getMessage() + ".\n";
            err += "Was trying to call URL " + url;

            System.err.println(err);
            throw new LHLookupException(exn, LHLookupExceptionReason.IO_FAILURE, err);
        }

        // Check response code.
        if (response.code() == 404) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OBJECT_NOT_FOUND,
                "Could not find WFSpec with guid " + guid + "."
            );
        } else if (response.code() != 200) {
            if (responseBody == null) {
                responseBody = "";
            }
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.OTHER_ERROR,
                "API Returned an error: " + String.valueOf(response.code()) + " " + responseBody
            );
        }

        WFRunSchema schema = BaseSchema.fromString(responseBody, WFRunSchema.class);
        if (schema == null) {
            throw new LHLookupException(
                null,
                LHLookupExceptionReason.INVALID_RESPONSE,
                "Got an invalid response: " + responseBody
            );
        }

        return new WFRun(schema, config);
    }

    public static String getContextString(WFRunSchema wfRun, WFTokenSchema token) {
        WFRunVariableContexSchema schema = WFRun.getContext(wfRun, token);
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

    public static WFRunSchema newSchemaFromRunRequest() {
        return null;
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
        WFRunSchema wfRun, EdgeConditionSchema condition, WFTokenSchema token
    ) throws VarSubOrzDash {
        if (condition == null) return true;
        Object lhs = getVariableSubstitution(wfRun, condition.leftSide, token);
        LHUtil.log("LHS is ", lhs, lhs.getClass());
        Object rhs = getVariableSubstitution(wfRun, condition.rightSide, token);
        switch (condition.comparator) {
            case LESS_THAN: return compare(lhs, rhs) < 0;
            case LESS_THAN_EQ: return compare(lhs, rhs) <= 0;
            case GREATER_THAN: return compare(lhs, rhs) > 0;
            case GRREATER_THAN_EQ: return compare(lhs, rhs) >= 0;
            case EQUALS: return lhs != null && lhs.equals(rhs);
            case NOT_EQUALS: return lhs != null && !lhs.equals(rhs);
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

        LHUtil.log("Left class: ", left.getClass());
        LHUtil.log("right class: ", right.getClass());
        try {
            LHUtil.log("Comparing", left, "to", right);
            int result = ((Comparable) left).compareTo((Comparable) right);
            LHUtil.log("got:", result);
            return result;
        } catch(Exception exn) {
            LHUtil.logError(exn.getMessage());
            throw new VarSubOrzDash(exn, "Failed comparing the provided values.");
        }
    }

    public static Object getVariableSubstitution(
            WFRunSchema wfRun, VariableAssignmentSchema var, WFTokenSchema token
    ) throws VarSubOrzDash {

        WFRunVariableContexSchema context = getContext(wfRun, token);
        if (var.literalValue != null) {
            LHUtil.log("returning literalvalue: ", var.literalValue.getClass());
            return var.literalValue;
        }

        // at this point, the thing must either come from a previous taskRun or
        // from a wfRunVariable.

        Object dataToParse = null;
        if (var.nodeName != null) {
            ArrayList<TaskRunSchema> taskRuns = context.nodeOutputs.get(var.nodeName);
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
            dataToParse = result;
        } else if (var.wfRunMetadata != null) {
            if (var.wfRunMetadata == WFRunMetadataEnum.WF_RUN_GUID) {
                return wfRun.guid;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_GUID) {
                return wfRun.wfSpecGuid;
            } else if (var.wfRunMetadata == WFRunMetadataEnum.WF_SPEC_NAME) {
                return wfRun.wfSpecName;
            }
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
            return JsonPath.parse(dataToParse.toString()).read(var.jsonPath);
        } catch(Exception exn) {
            throw new VarSubOrzDash(
                exn,
                "Specified jsonpath " + var.jsonPath + " failed to resolve on " + dataToParse
            );
        }
    }

    public static void mutateVariable(
        WFRunSchema wfRun,
        String varName,
        VariableMutationSchema mutation,
        WFSpec wfSpec,
        TaskRunSchema tr
    ) throws VarSubOrzDash {
        WFRunVariableDefSchema def = wfSpec.getModel().variableDefs.get(varName);
        String dataToParse = tr.toString();
        Object result;
        if (mutation.jsonPath != null) {
            result = JsonPath.parse(dataToParse).read(mutation.jsonPath);
        } else {
            result = mutation.literalValue;
        }
        Object original = wfRun.variables.get(varName);
        
        Class<?> defTypeCls = null;
        switch (def.type) {
            case STRING: defTypeCls = String.class; break;
            case OBJECT: defTypeCls = Map.class; break;
            case INT: defTypeCls = Integer.class; break;
            case DOUBLE: defTypeCls = Double.class; break;
            case BOOLEAN: defTypeCls = Boolean.class; break;
            case ARRAY: defTypeCls = List.class; break;
        }

        if (mutation.operation == VariableMutationOperation.SET) {
            // Do validation on the type.
            if (!defTypeCls.isInstance(result)) {
                throw new VarSubOrzDash(new Exception(),
                    "Expected type " + def.type + " but got " + result.getClass().getName() +
                    " substituting " + mutation.jsonPath + " on " + tr.toString()
                );
            }
            wfRun.variables.put(varName, result);
        } else if (mutation.operation == VariableMutationOperation.ADD) {
            if (def.type == WFRunVariableTypeEnum.BOOLEAN ||
                def.type == WFRunVariableTypeEnum.OBJECT
            ) {
                throw new VarSubOrzDash(
                    null,
                    "had an invalid wfspec. Need to catch this on wfspec validation"
                );
            } else if (def.type == WFRunVariableTypeEnum.STRING) {
                String orig = (String) original;
                orig = orig + ((String)result);
                wfRun.variables.put(varName, orig);
            } else if (def.type == WFRunVariableTypeEnum.INT) {
                Integer orig = (Integer) original;
                orig += (Integer) result;
                wfRun.variables.put(varName, orig);
            } else if (def.type == WFRunVariableTypeEnum.DOUBLE) {
                Double orig = (Double) original;
                orig += (Double) result;
                wfRun.variables.put(varName, orig);
            } else if (def.type == WFRunVariableTypeEnum.ARRAY) {
                @SuppressWarnings("unchecked")
                List<Object> orig = (List<Object>) original;
                orig.add(result);
                wfRun.variables.put(varName, orig);
            }
        
        } else {
            LHUtil.log(mutation, tr, "\n\nNeed to implement this new variable mutation operation");
        }
    }
}
