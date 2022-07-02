package little.horse.deployers.examples.kubernetes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.util.Constants;
import little.horse.common.util.LHClassLoadError;
import little.horse.common.util.LHUtil;
import little.horse.deployers.TaskDeployer;
import little.horse.deployers.examples.common.CustomTaskValidator;
import little.horse.deployers.examples.common.TaskImplTypeEnum;
import little.horse.deployers.examples.common.taskimpl.TaskWorker;
import little.horse.deployers.examples.kubernetes.specs.*;

public class K8sTaskDeployer implements TaskDeployer {

    public void deploy(TaskDef spec, DepInjContext config) throws LHConnectionError {
        KDConfig kdConfig = config.loadClass(KDConfig.class.getCanonicalName());
        Deployment dp = getK8sDeployment(spec, config, kdConfig);
        kdConfig.createDeployment(dp);
    }

    private Deployment getK8sDeployment(
        TaskDef spec, DepInjContext config, KDConfig kdConfig
    ) {
        K8sTaskDeployMeta meta;
        try {
            meta = BaseSchema.fromString(
                spec.deployMetadata, K8sTaskDeployMeta.class, config
            );
            if (meta.env == null) {
                meta.env = new HashMap<>();
            }
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

        dp.metadata.name = kdConfig.getK8sName(spec);
        dp.metadata.labels = new HashMap<String, String>();

        dp.metadata.labels.put("app", kdConfig.getK8sName(spec));
        dp.metadata.labels.put("io.littlehorse/deployedBy", "true");
        dp.metadata.labels.put("io.littlehorse/active", "true");
        dp.metadata.labels.put("io.littlehorse/taskDefId", spec.getObjectId());

        Container container = new Container();
        container.name = kdConfig.getK8sName(spec);
        container.image = meta.dockerImage;
        container.imagePullPolicy = "IfNotPresent";

        if (meta.taskType == TaskImplTypeEnum.JAVA) {
            container.command = Arrays.asList(
                "java", "-cp", "/javaInclude:/littleHorse.jar",
                TaskWorker.class.getCanonicalName()
            );
        } else if (meta.taskType == TaskImplTypeEnum.PYTHON) {
            container.command = Arrays.asList(
                "python", "-m", "executor"
            );
        }

        HashMap<String, String> env = config.getBaseEnv();
        env.put(
            Constants.KAFKA_APPLICATION_ID_KEY, "task-" + kdConfig.getK8sName(spec)
        );
        env.put(KDConstants.TASK_DEF_ID_KEY, spec.getObjectId());
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

        EnvEntry iid = EnvEntry.fromValueFrom(
            Constants.KAFKA_APPLICATION_IID_KEY, "metadata.name"
        );
        container.env.add(iid);

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.name = kdConfig.getK8sName(spec);
        template.metadata.labels = new HashMap<String, String>();
        template.metadata.namespace = (meta.namespace == null) ?
            kdConfig.getDefaultK8sNamespace(): meta.namespace;

        template.metadata.labels.put("app", kdConfig.getK8sName(spec));
        template.metadata.labels.put("io.littlehorse/deployedBy", "true");
        template.metadata.labels.put("io.littlehorse/active", "true");
        template.metadata.labels.put("io.littlehorse/taskDefId", spec.getObjectId());

        template.spec = new PodSpec();
        template.spec.containers = Arrays.asList(container);

        dp.spec.template = template;

        dp.spec.replicas = meta.replicas;
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = new HashMap<String, String>();
        dp.spec.selector.matchLabels.put("app", kdConfig.getK8sName(spec));
        dp.spec.selector.matchLabels.put("io.littlehorse/deployedBy", "true");
        dp.spec.selector.matchLabels.put("io.littlehorse/active", "true");
        dp.spec.selector.matchLabels.put("io.littlehorse/taskDefId", spec.getObjectId());

        return dp;
    }

    public void undeploy(TaskDef spec, DepInjContext config) throws LHConnectionError{
        KDConfig kdConfig = config.loadClass(KDConfig.class.getCanonicalName());
        kdConfig.deleteK8sDeployment("io.littlehorse/taskDefId", spec.getObjectId());
    }

    public void validate(TaskDef spec, DepInjContext config) throws LHValidationError {
        String message = null;
        if (spec.deployMetadata == null) {
            throw new LHValidationError("Must provide valid Docker validation!");
        }
        try {
            K8sTaskDeployMeta meta = BaseSchema.fromString(
                spec.deployMetadata, K8sTaskDeployMeta.class, config
            );
            if (meta.dockerImage == null) {
                message = "Must provide docker image!";
            }
            if (meta.env == null) {
                meta.env = new HashMap<>();
            }

            if (meta.taskType == TaskImplTypeEnum.JAVA) {
                if (meta.taskExecutorClassName == null) {
                    message = "Must provide task executor class for Java tasks!";
                }
            } else if (meta.taskType == TaskImplTypeEnum.PYTHON) {
                if (meta.pythonFunction == null || meta.pythonModule == null) {
                    message = "Must provide module and function for python tasks!";
                }
            }

            if (meta.customValidatorClassName != null) {
                CustomTaskValidator validator = LHUtil.loadClass(
                    meta.customValidatorClassName
                );
                validator.validate(spec, config);
            }
            KDConfig kdc = LHUtil.loadClass(KDConfig.class.getCanonicalName());
            new ObjectMapper(new YAMLFactory()).writeValueAsString(
                getK8sDeployment(spec, config, kdc)
            );
        } catch (LHSerdeError exn) {
            exn.printStackTrace();
            message = 
                "Failed unmarshalling task deployment metadata: " + exn.getMessage();
        } catch (LHClassLoadError exn) {
            exn.printStackTrace();
            message = "Could not load secondary validator class! " + exn.getMessage();
        } catch (Exception exn) {
            exn.printStackTrace();
            message = "Got unexpected error: " + exn.getMessage();
        }

        if (message != null) {
            throw new LHValidationError(message);
        }
    }
}
