package little.horse.sdk.local;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import little.horse.common.objects.metadata.WFSpec;
import little.horse.sdk.LHCompileException;
import little.horse.sdk.LHTaskFunction;
import little.horse.sdk.LHTaskOutput;
import little.horse.sdk.LHThreadContext;
import little.horse.sdk.LHVariable;
import little.horse.sdk.sdk.SpecBuilderTaskOutput;

public class LocalThreadContext implements LHThreadContext {
    private HashMap<String, Object> taskRuns;
    private HashMap<String, Object> variables;

    public LocalThreadContext() {
        this.variables = new HashMap<>();
        this.taskRuns = new HashMap<>();
    }

    public LHTaskOutput execute(Object task, Object...input) {
        int nodeSize = taskRuns.size();
        Method taskMethod = getTaskMethod(task);
        taskMethod.setAccessible(true);

        String key = String.valueOf(nodeSize) + "-" + taskMethod.getName();

        try {
            taskRuns.put(key, taskMethod.invoke(task, input));
        } catch (IllegalAccessException|InvocationTargetException exn) {
            throw new LHCompileException(exn.getMessage());
        }

        return new SpecBuilderTaskOutput(key);
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

    public void assign(LocalVariable var, Object output) {
        if (output instanceof LHTaskOutput) {
            LHTaskOutput lto = LHTaskOutput.class.cast(output);
            variables.put(var.getName(), taskRuns.get(lto.getNodeName()));
        } else {
            variables.put(var.getName(), output);
        }
    }

    public <T> LHVariable addVariable(String name, Class<T> cls) {
        variables.put(name, null);
        return new LocalVariable(name, this);
    }

    public WFSpec compile() {
        return null;
    }

    public void printout() {
        System.out.println(variables.toString());
        System.out.println(taskRuns.toString());
    }
}
