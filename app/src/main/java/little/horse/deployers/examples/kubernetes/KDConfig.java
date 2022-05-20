package little.horse.deployers.examples.kubernetes;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;


import little.horse.common.DepInjContext;
import little.horse.common.exceptions.LHConnectionError;
import little.horse.common.objects.metadata.POSTable;
import little.horse.common.objects.metadata.TaskDef;
import little.horse.common.objects.metadata.WFSpec;
import little.horse.common.util.LHDatabaseClient;
import little.horse.common.util.LHUtil;
import little.horse.deployers.examples.common.taskimpl.JavaTask;
import little.horse.deployers.examples.kubernetes.specs.Deployment;


public class KDConfig {
    private String wfSpecId;
    private String taskDefId;
    private String dockerHost;
    private String taskExecutorClassName;
    private int numThreads;
    private String k8sNamePrefix;
    private String defaultK8sNamespace;
    
    public KDConfig() {
        wfSpecId = System.getenv(KDConstants.WF_SPEC_ID_KEY);
        taskDefId = System.getenv(KDConstants.TASK_DEF_ID_KEY);
        taskExecutorClassName = System.getenv().get(
            KDConstants.TASK_EXECUTOR_CLASS_KEY
        );
        numThreads = Integer.valueOf(System.getenv().getOrDefault(
            KDConstants.TASK_EXECUTOR_THREADS_KEY, "10")
        );
        k8sNamePrefix = System.getenv().getOrDefault(
            KDConstants.K8S_NAME_PREFIX_KEY, ""
        );
        defaultK8sNamespace = System.getenv().getOrDefault(
            KDConstants.DEFAULT_K8S_NAMESPACE_KEY, "default"
        );
    }

    public String getDockerHost() {
        return this.dockerHost;
    }

    public String getWFSpecId() {
        return this.wfSpecId;
    }

    public JavaTask getTaskExecutor() {
        return LHUtil.loadClass(taskExecutorClassName);
    }

    public int getNumThreads() {
        return numThreads;
    }

    public String getTaskDefId() {
        return taskDefId;
    }

    public WFSpec lookupWFSpecOrDie(DepInjContext config) {
        WFSpec wfSpec = null;
        try {
            wfSpec = LHDatabaseClient.getByNameOrId(
                this.getWFSpecId(), config, WFSpec.class
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
        }
        if (wfSpec == null) {
            throw new RuntimeException("Couldn't load wfSpec" + getWFSpecId());
        }
        return wfSpec;
    }

    public TaskDef lookupTaskDefOrDie(DepInjContext config) {
        TaskDef taskDef = null;
        try {
            taskDef = LHDatabaseClient.getByNameOrId(
                this.getTaskDefId(), config, TaskDef.class
            );
        } catch (LHConnectionError exn) {
            exn.printStackTrace();
        }
        if (taskDef == null) {
            throw new RuntimeException("Couldn't load taskDef: " + getTaskDefId());
        }
        return taskDef;
    }

    public String getK8sName(POSTable obj) {
        return LHUtil.toValidK8sName(k8sNamePrefix + "-" + obj.name);
    }

    public String getDefaultK8sNamespace() {
        return defaultK8sNamespace;
    }

    public void createDeployment(Deployment deployment) throws LHConnectionError {
        String yml = null;
        try {
            yml = new ObjectMapper(new YAMLFactory()).writeValueAsString(deployment);
        } catch(JsonProcessingException exn) {
            throw new RuntimeException(
                "This shouldn't be possible after validation"
            );
        }

        try {
            Process process = Runtime.getRuntime().exec("kubectl apply -f -");

            process.getOutputStream().write(yml.getBytes());
            process.getOutputStream().close();
            process.waitFor();

            BufferedReader input = new BufferedReader(
                new InputStreamReader(process.getInputStream())
            );
            String line = null;
            while ((line = input.readLine()) != null) {
                LHUtil.log(line);
            }

            BufferedReader error = new BufferedReader(
                new InputStreamReader(process.getErrorStream())
            );
            line = null;
            while ((line = error.readLine()) != null) {
                LHUtil.log(line);
            }
            if (process.exitValue() != 0) {
                throw new RuntimeException(String.format(
                    "Got nonzero exit value %d!", process.exitValue()
                ));
            }
        } catch(Exception exn) {
            exn.printStackTrace();
            throw new LHConnectionError(
                exn, "Had an issue deploying: " + exn.getMessage()
            );
        }
    }

    public void deleteK8sDeployment(String labelKey, String labelValue)
    throws LHConnectionError {
        try {
            Process process = Runtime.getRuntime().exec(String.format(
                "kubectl delete deploy -l%s=%s",
                labelKey, labelValue
            ));
            process.getOutputStream().close();
            process.waitFor();
            BufferedReader input = new BufferedReader(
                new InputStreamReader(process.getInputStream())
            );
            String line = null;
            while ((line = input.readLine()) != null) {
                LHUtil.log(line);
            }

            BufferedReader error = new BufferedReader(
                new InputStreamReader(process.getErrorStream())
            );
            line = null;
            while ((line = error.readLine()) != null) {
                LHUtil.log(line);
            }
            if (process.exitValue() != 0) {
                throw new RuntimeException(String.format(
                    "Got nonzero exit value %d!", process.exitValue()
                ));
            }

        } catch (Exception exn) {
            throw new LHConnectionError(
                exn,
                "Failed deleting deployment: " + exn.getMessage()
            );
        }
    }
}
