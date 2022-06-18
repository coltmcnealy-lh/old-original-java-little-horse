package io.littlehorse.deployers.examples.common.taskimpl.bashExecutor;

import java.util.ArrayList;
import java.util.regex.Matcher;
import io.littlehorse.common.LHConfig;
import io.littlehorse.common.exceptions.LHSerdeError;
import io.littlehorse.common.objects.BaseSchema;
import io.littlehorse.common.util.LHUtil;
import io.littlehorse.deployers.examples.common.DeployerConfig;
import io.littlehorse.deployers.examples.common.DeployerConstants;
import io.littlehorse.deployers.examples.common.taskimpl.JavaTask;
import io.littlehorse.deployers.examples.common.taskimpl.WorkerContext;
import io.littlehorse.scheduler.TaskScheduleRequest;

public class BashExecutor implements JavaTask {
    private BashTaskMetadata meta;

    public void init(DeployerConfig ddConfig, LHConfig config) {        
        try {
            meta = BaseSchema.fromString(
                System.getenv(DeployerConstants.TASK_EXECUTOR_META_KEY),
                BashTaskMetadata.class,
                config
            );
        } catch(LHSerdeError exn) {
            throw new RuntimeException("Invalid configuration!", exn);
        }
    }

    private static String castToString(Object obj) {
        if (obj instanceof String) {
            return String.class.cast(obj);
        }

        return String.valueOf(obj);
    }

    public Object executeTask(TaskScheduleRequest request, WorkerContext context)
    throws Exception {
        ArrayList<String> cmd = new ArrayList<>();

        for (String arg: meta.bashCommand) {
            Matcher varMatcher = BashValidator.VARIABLE_PATTERN.matcher(arg);
            Matcher metaVarMatcher = BashValidator.META_VARIABLE_PATTERN.matcher(arg);

            if (varMatcher.matches()) {
                String varName = arg.substring(2, arg.length() - 2);
                cmd.add(castToString(
                    request.variableSubstitutions.get(varName)
                ));
            } else if (metaVarMatcher.matches()) {
                String metaVarname = arg.substring(3, arg.length() - 3);
                if (metaVarname.equals("THREAD_RUN_ID")) {
                    cmd.add(String.valueOf(request.threadId));
                } else if (metaVarname.equals("TASK_RUN_NUMBER")) {
                    cmd.add(String.valueOf(request.taskRunPosition));
                } else if (metaVarname.equals("WF_RUN_ID")) {
                    cmd.add(request.wfRunId);
                } else {
                    throw new RuntimeException("Invalid metavarname: " + arg);
                }
            } else {
                cmd.add(arg);
            }
        }

        ProcessBuilder builder = new ProcessBuilder(cmd);
        Process proc;
        proc = builder.start();
        proc.getOutputStream().close();
        proc.waitFor();

        if (proc.exitValue() == 0) {
            context.log(LHUtil.inputStreamToString(proc.getErrorStream()));
            return LHUtil.inputStreamToString(proc.getInputStream());
        } else {
            throw new RuntimeException("Task Execution failed: " +
                LHUtil.inputStreamToString(proc.getErrorStream()) + "\n\n" +
                LHUtil.inputStreamToString(proc.getInputStream())
            );
        }
    }
}
