package io.littlehorse.sdkprototype;

public interface LHTaskOutput {
    public String getNodeName();

    public LHTaskOutput doExcept(ExceptionHandlerThread thread);
}
