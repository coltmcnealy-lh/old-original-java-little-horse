package little.horse.common.util.K8sStuff;

import java.util.HashMap;

public class DeploymentMetadata {
    public String name;
    public HashMap<String, String> labels;
    public String namespace;
}
