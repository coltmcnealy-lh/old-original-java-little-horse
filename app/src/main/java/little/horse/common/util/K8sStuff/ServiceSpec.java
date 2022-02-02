package little.horse.common.util.K8sStuff;

import java.util.ArrayList;
import java.util.HashMap;

public class ServiceSpec {
    public HashMap<String, String> selector;
    public ArrayList<ServicePort> ports;
}
