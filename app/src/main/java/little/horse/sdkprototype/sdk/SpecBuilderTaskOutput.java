package little.horse.sdkprototype.sdk;

import little.horse.sdkprototype.ExceptionHandlerThread;
import little.horse.sdkprototype.LHTaskOutput;

public class SpecBuilderTaskOutput implements LHTaskOutput {
    private String nodeName;
    private SpecBuilderThreadContext context; 

    // Edit this constructor to take in the SpecBuilderThreadContext?
    public SpecBuilderTaskOutput(String nodeName, SpecBuilderThreadContext context) {
        this.nodeName = nodeName;
        this.context = context;
    }

    public String getNodeName() {
        return nodeName;
    }

    public LHTaskOutput doExcept(ExceptionHandlerThread thread) {
        context.addExceptionHandler(this, thread);
        return this;
    }

}
