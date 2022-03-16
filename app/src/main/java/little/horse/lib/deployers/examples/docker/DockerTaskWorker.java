package little.horse.lib.deployers.examples.docker;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.LHUtil;
import little.horse.lib.worker.TaskWorker;
import little.horse.lib.worker.examples.docker.DockerTaskExecutor;

public class DockerTaskWorker {
    private TaskWorker worker;
    private DockerTaskExecutor executor;

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

    public static void main(String[] args) {
        LHUtil.log("Running Docker Task Worker!");
        Config config = new Config();
        DDConfig ddConfig = new DDConfig();
        DockerTaskWorker dtw = new DockerTaskWorker(ddConfig, config);
        dtw.run();
    }
}
