package little.horse.lib.worker.examples.docker.bashExecutor;

import java.util.ArrayList;
import java.util.regex.Matcher;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.examples.docker.DDConfig;
import little.horse.lib.deployers.examples.docker.DDConstants;
import little.horse.lib.worker.WorkerContext;
import little.horse.lib.worker.examples.docker.DockerTaskExecutor;
import little.horse.workflowworker.TaskScheduleRequest;

public class BashExecutor implements DockerTaskExecutor {
    private BashTaskMetadata meta;

    public void init(DDConfig ddConfig, DepInjContext config) {        
        try {
            meta = BaseSchema.fromString(
                System.getenv(DDConstants.TASK_EXECUTOR_META_KEY),
                BashTaskMetadata.class,
                config
            );
        } catch(LHSerdeError exn) {
            throw new RuntimeException("Invalid configuration!", exn);
        }
    }

    public Object executeTask(TaskScheduleRequest request, WorkerContext context)
    throws Exception {
        ArrayList<String> cmd = new ArrayList<>();

        for (String arg: meta.bashCommand) {
            Matcher varMatcher = BashValidator.VARIABLE_PATTERN.matcher(arg);
            Matcher metaVarMatcher = BashValidator.META_VARIABLE_PATTERN.matcher(arg);

            if (varMatcher.matches()) {
                String varName = arg.substring(2, arg.length() - 2);
                cmd.add(String.class.cast(
                    request.variableSubstitutions.get(varName)
                ));
            } else if (metaVarMatcher.matches()) {
                String metaVarname = arg.substring(3, arg.length() - 3);
                if (metaVarname.equals("THREAD_RUN_ID")) {
                    cmd.add(String.valueOf(request.threadRunNumber));
                } else if (metaVarname.equals("TASK_RUN_NUMBER")) {
                    cmd.add(String.valueOf(request.taskRunNumber));
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
