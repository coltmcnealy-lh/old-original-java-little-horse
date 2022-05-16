package little.horse.deployers.examples.docker;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;
import little.horse.deployers.examples.common.TaskImplTypeEnum;

public class DockerTaskDeployMetadata extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String customValidatorClassName;
    
    public TaskImplTypeEnum taskType = TaskImplTypeEnum.JAVA;

    // Java tasks
    public String taskExecutorClassName;

    // Python tasks
    public String pythonModule;
    public String pythonFunction;

    public HashMap<String, String> env;
}
