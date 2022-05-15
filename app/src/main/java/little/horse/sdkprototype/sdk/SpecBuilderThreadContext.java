package little.horse.sdkprototype.sdk;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import little.horse.common.DepInjContext;
import little.horse.common.objects.metadata.Edge;
import little.horse.common.objects.metadata.ExceptionHandlerSpec;
import little.horse.common.objects.metadata.Node;
import little.horse.common.objects.metadata.NodeType;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.ThreadSpec;
import little.horse.common.objects.metadata.VariableAssignment;
import little.horse.common.objects.metadata.VariableMutation;
import little.horse.common.objects.metadata.VariableMutationOperation;
import little.horse.common.objects.metadata.WFRunVariableDef;
import little.horse.common.objects.metadata.WFRunVariableTypeEnum;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.deployers.examples.common.taskimpl.bashExecutor.BashExecutor;
import little.horse.deployers.examples.common.taskimpl.bashExecutor.BashTaskMetadata;
import little.horse.deployers.examples.common.taskimpl.bashExecutor.BashValidator;
import little.horse.deployers.examples.docker.DockerTaskDeployMetadata;
import little.horse.deployers.examples.docker.DockerTaskDeployer;
import little.horse.deployers.examples.docker.DockerWorkflowDeployer;
import little.horse.sdkprototype.ExceptionHandlerThread;
import little.horse.sdkprototype.LHCompileException;
import little.horse.sdkprototype.LHTaskFunction;
import little.horse.sdkprototype.LHTaskOutput;
import little.horse.sdkprototype.LHThreadContext;
import little.horse.sdkprototype.LHVariable;

public class SpecBuilderThreadContext implements LHThreadContext {
    private WFSpec spec;
    public ThreadSpec entrypoint;
    private String lastNodeName;
    private ArrayList<TaskDef> taskDefs;
    private DepInjContext config;

    public SpecBuilderThreadContext(DepInjContext config, String name) {
        this.spec = new WFSpec();
        this.spec.threadSpecs = new HashMap<>();
        this.spec.entrypointThreadName = "entrypoint";
        this.entrypoint = new ThreadSpec();
        this.spec.threadSpecs.put("entrypoint", entrypoint);
        this.entrypoint.variableDefs = new HashMap<>();
        this.entrypoint.nodes = new HashMap<>();
        this.entrypoint.edges = new ArrayList<>();
        this.lastNodeName = null;
        spec.name = name;
        this.taskDefs = new ArrayList<>();
        spec.setWfDeployerClassName(DockerWorkflowDeployer.class.getCanonicalName());
        this.config = config;

        spec.setConfig(config);

    }

    public List<TaskDef> getTaskDefs() {
        return taskDefs;
    }

    public LHTaskOutput execute(Object task, Object...args) {
        Method taskMethod = getTaskMethod(task);

        if (taskMethod == null) {
            throw new LHCompileException(
                task.getClass().getCanonicalName() +
                "provided no methods with the LHTaskFunction annotation!"
            );
        }

        Node node = new Node();
        node.nodeType = NodeType.TASK;
        int nodeSize = entrypoint.nodes.size();
        node.name = String.valueOf(nodeSize) + "-" + taskMethod.getName();

        // Note that we could get the return type to do some type validation, and we
        // will do that in the future, but for now we gotta get something working
        // so I can check my hinge account

        // get the input types and add input variables
        node.variables = new HashMap<>();
        ArrayList<Object> inputs = new ArrayList<>();
        for (Object thing: args) {
            inputs.add(thing);
        }
        if (inputs.size() != taskMethod.getParameterCount()) {
            throw new LHCompileException(
                "Provided " + inputs.size() + " params but expected " +
                taskMethod.getParameterCount() + "!"
            );
        }

        int i = 0;
        for (Parameter param: taskMethod.getParameters()) {
            Object arg = inputs.get(i++);
            node.variables.put(param.getName(), assignVariable(arg));
        }

        // Add edges
        if (lastNodeName != null) {
            Edge edge = new Edge();
            edge.sourceNodeName = lastNodeName;
            edge.sinkNodeName = node.name;
            entrypoint.edges.add(edge);
        }

        lastNodeName = node.name;

        // form a taskdef if necessary
        node.taskDefName = addTaskDef(task, taskMethod);

        entrypoint.nodes.put(node.name, node);
        node.threadSpec = entrypoint;

        // TODO: return a SpecBuilderTaskOutput instead of returning null.
        SpecBuilderTaskOutput output = new SpecBuilderTaskOutput(node.name, this);
        return output;
    }

    public void addExceptionHandler(LHTaskOutput taskOutput, ExceptionHandlerThread thread) {
        // TODO: implement this.

        // Step 1: Create a new SpecBuilderThreadContext sbtc
        SpecBuilderThreadContext sbtc = new SpecBuilderThreadContext(config, "exception-handler");

        // Step 2: Call thread.operate(sbtc);
        thread.operate(sbtc);

        // Step 3: Get the result of sbtc
        ThreadSpec exceptionHandler = sbtc.entrypoint;

        // Step 4: Add the result of sbtc to WFSpec.threadSpecs
        spec.threadSpecs.put("exception-handler", exceptionHandler);

        // Step 5: Set taskOutput.getNode().baseExceptionHandler = <<the name>>
        Node node = entrypoint.nodes.get(taskOutput.getNodeName());
        node.baseExceptionhandler = new ExceptionHandlerSpec();
        node.baseExceptionhandler.handlerThreadSpecName = "exception-handler";

    }

    private VariableAssignment assignVariable(Object arg) {
        VariableAssignment ass = new VariableAssignment();
        if (arg instanceof LHVariable) {
            LHVariable lvar = LHVariable.class.cast(arg);
            ass.wfRunVariableName = lvar.getName();
            ass.jsonPath = null;
        } else {
            ass.literalValue = arg;
        }
        // let's see who's first to refactor this var name :)
        return ass;
    }

    private String addTaskDef(Object task, Method method) {
        TaskDef td = new TaskDef();
        String name = "task-" + method.getDeclaringClass().getSimpleName() + "-"
            + method.getName();
        td.name = name;
        td.setTaskDeployerClassName(DockerTaskDeployer.class.getCanonicalName());

        DockerTaskDeployMetadata meta = new DockerTaskDeployMetadata();
        meta.dockerImage = "little-horse-api:latest";
        meta.customValidatorClassName = 
            BashValidator.class.getCanonicalName();
        meta.taskExecutorClassName = BashExecutor.class.getCanonicalName();
        BashTaskMetadata secondMeta = new BashTaskMetadata();
        secondMeta.bashCommand = Arrays.asList(
            "java", "-cp", "/littleHorse.jar",
            method.getDeclaringClass().getCanonicalName()
        );
        meta.metadata = secondMeta.toString();
        td.deployMetadata = meta.toString();
        td.setConfig(config);

        taskDefs.add(td);
        return name;
    }

    private Method getTaskMethod(Object supposedTask) {
        Method out = null;

        for (Method candidate: supposedTask.getClass().getDeclaredMethods()) {
            if (!candidate.isAnnotationPresent(LHTaskFunction.class)) continue;

            if (out != null) {
                throw new LHCompileException(
                    "Class " + supposedTask.getClass().getCanonicalName() + "has " +
                    "more than one LHTaskFunction methods!"
                );
            }
            out = candidate;
        }

        return out;
    }

    public void assign(SpecBuilderVariable var, Object output) {
        if (output instanceof LHTaskOutput) {
            assignOutput(var, LHTaskOutput.class.cast(output));
        } else {
            assignLiteral(var, output);
        }
    }

    private void assignLiteral(SpecBuilderVariable var, Object val) {

    }

    private void assignOutput(SpecBuilderVariable var, LHTaskOutput output) {
        Node node = entrypoint.nodes.get(output.getNodeName());
        if (node.variableMutations == null) {
            node.variableMutations = new HashMap<>();
        }

        VariableMutation mutation = new VariableMutation();
        mutation.operation = VariableMutationOperation.ASSIGN;
        mutation.jsonPath = null; // Could do something fancier here

        node.variableMutations.put(var.getName(), mutation);
    }

    public <T> LHVariable addVariable(String name, Class<T> cls) {
        WFRunVariableDef def = new WFRunVariableDef();

        if (cls == Integer.class) def.type = WFRunVariableTypeEnum.INT;
        else if (cls == List.class) def.type = WFRunVariableTypeEnum.ARRAY;
        else if (cls == Double.class) def.type = WFRunVariableTypeEnum.FLOAT;
        else if (cls == Boolean.class) def.type = WFRunVariableTypeEnum.BOOLEAN;
        else if (cls == String.class) def.type = WFRunVariableTypeEnum.STRING;
        else def.type = WFRunVariableTypeEnum.OBJECT;

        this.entrypoint.variableDefs.put(name, def);

        return new SpecBuilderVariable(this, name);
    }

    public WFSpec compile() {
        return this.spec;
    }
    
}
