import little.horse.common.DepInjContext;
import little.horse.deployers.examples.common.DeployerConfig;
import little.horse.deployers.examples.common.taskimpl.WorkerContext;
import little.horse.deployers.examples.common.taskimpl.JavaTask;
import little.horse.scheduler.TaskScheduleRequest;


// haven't figured out how to get VSCode to have different classpaths for
// sub directories. This wouldn't be a problem in production as normally you would
// have tasks in their own separate VSCode project but here we have it in a totally
// unrelated one as a standalone java file.

public class MyTask implements JavaTask {
    public void init(DeployerConfig ddConfig, DepInjContext config) {

    }

    public Object executeTask(TaskScheduleRequest request, WorkerContext context) {
        return "Hello there from a Java task!";
    }
}