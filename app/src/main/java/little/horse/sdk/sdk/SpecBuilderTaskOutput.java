package little.horse.sdk.sdk;

import little.horse.sdk.ExceptionHandlerThread;
import little.horse.sdk.LHTaskOutput;

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
