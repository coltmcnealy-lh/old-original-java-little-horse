package little.horse.lib.deployers.examples.kubernetes.specs;

import java.util.List;

public class Container {
    public String name;
    public String image;
    public List<String> command;
    public List<ContainerPort> ports;
    public List<EnvEntry> env;
    public String imagePullPolicy;

    public Probe startupProbe;
    public Probe livenessProbe;
    public Probe readinessProbe;
}
