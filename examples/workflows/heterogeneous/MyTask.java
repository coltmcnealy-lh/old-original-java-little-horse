import little.horse.common.DepInjContext;
import little.horse.lib.deployers.examples.docker.DDConfig;
import little.horse.lib.deployers.examples.docker.DDConstants;
import little.horse.lib.worker.WorkerContext;
import little.horse.lib.worker.examples.docker.JavaTaskExecutor;
import little.horse.workflowworker.TaskScheduleRequest;


// haven't figured out how to get VSCode to have different classpaths for
// sub directories. This wouldn't be a problem in production as normally you would
// have tasks in their own separate VSCode project but here we have it in a totally
// unrelated one as a standalone java file.

public class MyTask implements DockerTaskExecutor {
    public void init(DDConfig ddConfig, DepInjContext config) {

    }

    public Object executeTask(TaskScheduleRequest request, WorkerContext context) {
        return "Hello there from a Java task!";
    }
}