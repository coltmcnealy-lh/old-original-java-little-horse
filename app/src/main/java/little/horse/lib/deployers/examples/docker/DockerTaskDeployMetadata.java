package little.horse.lib.deployers.examples.docker;

import java.util.HashMap;

import little.horse.common.objects.BaseSchema;

public class DockerTaskDeployMetadata extends BaseSchema {
    public String dockerImage;
    public String metadata;
    public String secondaryValidatorClassName;
    public String taskExecutorClassName;

    public HashMap<String, String> env;
}
