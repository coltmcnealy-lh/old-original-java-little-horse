package little.horse.sdk.sdk;

import little.horse.sdk.LHTaskOutput;

public class SpecBuilderTaskOutput implements LHTaskOutput {
    private String nodeName;

    public SpecBuilderTaskOutput(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

}
