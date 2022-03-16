package little.horse.lib.deployers.enterprise.kubernetes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.Constants;
import little.horse.lib.deployers.enterprise.kubernetes.specs.*;
import little.horse.lib.deployers.TaskDeployer;
import little.horse.lib.deployers.enterprise.kubernetes.specs.Container;
import little.horse.lib.deployers.examples.docker.DockerTaskWorker;

public class K8sTaskDeployer implements TaskDeployer {

    public void deploy(TaskDef spec, Config config) throws LHConnectionError {
        KDConfig kdConfig = config.loadClass(KDConfig.class.getCanonicalName());
        Deployment dp = getK8sDeployment(spec, config, kdConfig);

        kdConfig.createDeployment(dp);
    }

    private Deployment getK8sDeployment(
        TaskDef spec, Config config, KDConfig kdConfig
    ) {
        K8sTaskDeployMeta meta;
        try {
            meta = BaseSchema.fromString(
                spec.deployMetadata, K8sTaskDeployMeta.class, config
            );
        } catch (LHSerdeError exn) {
            throw new RuntimeException(
                "Should be impossible--didn't we already validate this?", exn
            );
        }

        Deployment dp = new Deployment();
        dp.metadata = new DeploymentMetadata();
        dp.spec = new DeploymentSpec();
        dp.kind = "Deployment";
        dp.apiVersion = "apps/v1";

        dp.metadata.namespace = (meta.namespace == null) ?
            kdConfig.getDefaultK8sNamespace(): meta.namespace;

        dp.metadata.name = spec.name;
        dp.metadata.labels = new HashMap<String, String>();

        dp.metadata.labels.put("app", kdConfig.getK8sName(spec));
        dp.metadata.labels.put("io.littlehorse/deployedBy", "true");
        dp.metadata.labels.put("io.littlehorse/active", "true");
        dp.metadata.labels.put("io.littlehorse/taskDefId", spec.getId());

        Container container = new Container();
        container.name = kdConfig.getK8sName(spec);
        container.image = meta.dockerImage;
        container.imagePullPolicy = "IfNotPresent";
        container.command = Arrays.asList(
            "java", DockerTaskWorker.class.getCanonicalName()
        );

        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, spec.name);
        env.put(KDConstants.TASK_DEF_ID_KEY, spec.getId());
        env.put(KDConstants.TASK_EXECUTOR_META_KEY, meta.metadata);
        env.put(KDConstants.TASK_EXECUTOR_CLASS_KEY, meta.taskExecutorClassName);

        for (Map.Entry<String, String> envEntry: meta.env.entrySet()) {
            env.put(envEntry.getKey(), envEntry.getValue());
        }
        container.env = new ArrayList<EnvEntry>();

        for (Map.Entry<String, String> envEntry: env.entrySet()) {
            container.env.add(new EnvEntry(
                envEntry.getKey(), envEntry.getValue()
            ));
        }

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = kdConfig.getK8sName(spec);
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = (meta.namespace == null) ?
            kdConfig.getDefaultK8sNamespace(): meta.namespace;

        template.metadata.labels.put("app", kdConfig.getK8sName(spec));
        template.metadata.labels.put("io.littlehorse/deployedBy", "true");
        template.metadata.labels.put("io.littlehorse/active", "true");
        template.metadata.labels.put("io.littlehorse/taskDefId", spec.getId());

        template.spec = new PodSpec();
        template.spec.containers = Arrays.asList(container);

        dp.spec.template = template;
        dp.spec.replicas = 1; // TODO: Support more, which will need StatefulSet
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", kdConfig.getK8sName(spec));
        dp.spec.selector.matchLabels.put("io.littlehorse/deployedBy", "true");
        dp.spec.selector.matchLabels.put("io.littlehorse/active", "true");
        dp.spec.selector.matchLabels.put("io.littlehorse/taskDefId", spec.getId());

        return dp;
    }

    public void undeploy(TaskDef spec, Config config) throws LHConnectionError{
        KDConfig kdConfig = config.loadClass(KDConfig.class.getCanonicalName());
        kdConfig.deleteK8sDeployment("io.littlehorse.taskDefId", spec.getId());
    }

    public void validate(TaskDef spec, Config config) {

    }
}
