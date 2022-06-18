package io.littlehorse.sdkprototype;

import io.littlehorse.sdkprototype.sdk.ExceptionHandlerThread;

public interface LHTaskOutput {
    public String getNodeName();

    public LHTaskOutput doExcept(ExceptionHandlerThread thread);
}
