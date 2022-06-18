package io.littlehorse.sdkprototype.examples;

import io.littlehorse.common.LHConfig;
import io.littlehorse.sdkprototype.LHTaskFunction;
import io.littlehorse.sdkprototype.LHTaskOutput;
import io.littlehorse.sdkprototype.LHVariable;
import io.littlehorse.sdkprototype.sdk.SpecBuilderThreadContext;

class MyTask {
    @LHTaskFunction
    public Integer doTask(String input) {
        return input.length();
    }
}

public class Basic {
    public static void main(String[] args) {
        SpecBuilderThreadContext wf = new SpecBuilderThreadContext(
            new LHConfig(), "my-wf"
        );

        LHVariable lenVar = wf.addVariable("len", Integer.class);
        LHTaskOutput taskOneOutput = wf.execute(
            new MyTask(),
            "this is the input to the first execution"
        );
        lenVar.assign(taskOneOutput);

        
        wf.execute(new MyTask(), "another task!");

        System.out.println(wf.compile().toString());
    }
}
