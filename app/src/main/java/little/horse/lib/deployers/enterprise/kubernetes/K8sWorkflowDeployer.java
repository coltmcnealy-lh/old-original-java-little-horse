package little.horse.lib.deployers.enterprise.kubernetes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import little.horse.common.Config;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.exceptions.LHSerdeError;
import little.horse.common.exceptions.LHValidationError;
import little.horse.common.objects.BaseSchema;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.Constants;
import little.horse.common.util.LHClassLoadError;
import little.horse.common.util.LHUtil;
import little.horse.lib.deployers.WorkflowDeployer;
import little.horse.lib.deployers.enterprise.kubernetes.specs.Container;
import little.horse.lib.deployers.enterprise.kubernetes.specs.Deployment;
import little.horse.lib.deployers.enterprise.kubernetes.specs.DeploymentMetadata;
import little.horse.lib.deployers.enterprise.kubernetes.specs.DeploymentSpec;
import little.horse.lib.deployers.enterprise.kubernetes.specs.EnvEntry;
import little.horse.lib.deployers.enterprise.kubernetes.specs.PodSpec;
import little.horse.lib.deployers.enterprise.kubernetes.specs.Selector;
import little.horse.lib.deployers.enterprise.kubernetes.specs.Template;
import little.horse.lib.deployers.examples.docker.DockerWorkflowWorker;

public class K8sWorkflowDeployer implements WorkflowDeployer {

    public void deploy(WFSpec spec, Config config) throws LHConnectionError {
        KDConfig kdConfig = LHUtil.loadClass(KDConfig.class.getCanonicalName());
        Deployment dp = getK8sDeployment(spec, config, kdConfig);

        kdConfig.createDeployment(dp);
    }

    private Deployment getK8sDeployment(
        WFSpec spec, Config config, KDConfig kdConfig
    ) throws LHConnectionError {
        K8sWorkflowDeployMeta meta;
        try {
            meta = BaseSchema.fromString(
                spec.deployMetadata, K8sWorkflowDeployMeta.class, config
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
        dp.metadata.labels.put("io.littlehorse/wfSpecId", spec.getId());

        Container container = new Container();
        container.name = kdConfig.getK8sName(spec);
        container.image = meta.dockerImage;
        container.imagePullPolicy = "IfNotPresent";
        container.command = Arrays.asList(
            "java", DockerWorkflowWorker.class.getCanonicalName()
        );

        HashMap<String, String> env = config.getBaseEnv();
        env.put(Constants.KAFKA_APPLICATION_ID_KEY, "wf-" + spec.name);
        env.put(KDConstants.WF_SPEC_ID_KEY, spec.getId());
        container.env = new ArrayList<EnvEntry>();
        for (Map.Entry<String, String> envEntry: env.entrySet()) {
            container.env.add(new EnvEntry(
                envEntry.getKey(), envEntry.getValue()
            ));
        }

        HashMap<String, String> labels = new HashMap<>();
        labels.put("io.littlehorse/deployedBy", "true");
        labels.put("io.littlehorse/wfSpecId", spec.getId());
        labels.put("io.littlehorse/wfSpecName", spec.name);
        labels.put("io.littlehorse/active", "true");

        Template template = new Template();
        template.metadata = new DeploymentMetadata();
        template.metadata.labels = labels;
        template.metadata.name = kdConfig.getK8sName(spec);
        template.metadata.namespace = (meta.namespace == null) ?
            kdConfig.getDefaultK8sNamespace(): meta.namespace;
        
        template.spec = new PodSpec();
        template.spec.containers = Arrays.asList(container);

        dp.spec.template = template;
        dp.spec.replicas = 1; // TODO: Support more, which will need StatefulSet
        dp.spec.selector = new Selector();
        dp.spec.selector.matchLabels = labels;        

        return dp;
    }

    public void undeploy(WFSpec spec, Config config) throws LHConnectionError{
        KDConfig kdConfig = config.loadClass(KDConfig.class.getCanonicalName());
        kdConfig.deleteK8sDeployment("io.littlehorse.wfSpecId", spec.getId());
    }

    public void validate(WFSpec spec, Config config) throws LHValidationError {
        String message = null;
        if (spec.deployMetadata == null) {
            throw new LHValidationError("Must provide valid Docker validation!");
        }
        try {
            K8sWorkflowDeployMeta meta = BaseSchema.fromString(
                spec.deployMetadata, K8sWorkflowDeployMeta.class, config
            );
            if (meta.dockerImage == null) {
                message = "Must provide docker image for Workflow Worker!";
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
