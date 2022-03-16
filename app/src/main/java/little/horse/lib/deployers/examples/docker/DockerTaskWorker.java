package little.horse.lib.deployers.examples.docker;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.lib.worker.TaskExecutor;
import little.horse.lib.worker.TaskWorker;

public class DockerTaskWorker {
    private TaskWorker worker;
    private TaskExecutor executor;

    public DockerTaskWorker(DDConfig ddConfig, Config config) {
        this.executor = ddConfig.getTaskExecutor();
        this.executor.init(ddConfig, config);
        TaskDef taskDef = ddConfig.lookupTaskDefOrDie(config);
        try {
            this.worker = new TaskWorker(
                config, taskDef.name, executor, ddConfig.getNumThreads()
            );
        } catch (LHConnectionError exn) {
            throw new RuntimeException(exn);
        }
    }

    public void run() {
        worker.run();
    }
}
