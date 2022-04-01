package little.horse.lib.deployers.examples.kubernetes.specs;

import java.util.ArrayList;
import java.util.HashMap;

public class ServiceSpec {
    public HashMap<String, String> selector;
    public ArrayList<ServicePort> ports;
}
