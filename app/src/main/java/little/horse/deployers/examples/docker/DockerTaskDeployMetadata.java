package little.horse.deployers.examples.docker;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class DockerTaskDeployMetadata extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String customValidatorClassName;
    public String taskExecutorClassName;

    public HashMap<String, String> env;
}
