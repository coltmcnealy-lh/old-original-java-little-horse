package little.horse.lib.K8sStuff;

import java.util.ArrayList;

public class Container {
    public String name;
    public String image;
    public ArrayList<String> command;
    public ArrayList<ContainerPort> ports;
    public ArrayList<EnvEntry> env;
}