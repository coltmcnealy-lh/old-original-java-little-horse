package little.horse.lib.K8sStuff;

import java.util.HashMap;

public class DeploymentMetadata {
    public String name;
    public HashMap<String, String> labels;
    public String namespace;
}
