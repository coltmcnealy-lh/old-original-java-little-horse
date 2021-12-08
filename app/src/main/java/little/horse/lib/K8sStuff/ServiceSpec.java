package little.horse.lib.K8sStuff;

import java.util.ArrayList;

public class ServiceSpec {
    public Selector selector;
    public ArrayList<Port> ports;
}
