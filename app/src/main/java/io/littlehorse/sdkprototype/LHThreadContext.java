package io.littlehorse.sdkprototype;

import io.littlehorse.common.objects.metadata.WFSpec;

public interface LHThreadContext {
    public LHTaskOutput execute(Object task, Object...input);
    public <T> LHVariable addVariable(String name, Class<T> cls);

    public WFSpec compile();
}
