package little.horse.examples.bashExecutor;

import java.util.ArrayList;
import java.util.regex.Matcher;

import little.horse.api.runtime.TaskScheduleRequest;
import little.horse.common.Config;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.util.LHUtil;
import little.horse.examples.deployers.docker.DDConfig;
import little.horse.examples.deployers.docker.DDConstants;
import little.horse.lib.worker.TaskExecutor;
import little.horse.lib.worker.WorkerContext;

public class BashExecutor implements TaskExecutor {
    private BashTaskMetadata meta;

    public void init(DDConfig ddConfig, Config config) {        
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
            Matcher m = BashValidator.VARIABLE_PATTERN.matcher(arg);
            if (m.matches()) {
                String varName = arg.substring(2, arg.length() - 2);
                cmd.add(String.class.cast(
                    request.variableSubstitutions.get(varName)
                ));
            } else {
                cmd.add(arg);
            }
        }

        ProcessBuilder builder = new ProcessBuilder(cmd);
        Process proc;
        proc = builder.start();
        proc.getOutputStream().close();
        proc.waitFor();

        context.log(LHUtil.inputStreamToString(proc.getErrorStream()));
        return LHUtil.inputStreamToString(proc.getInputStream());
    }
}
