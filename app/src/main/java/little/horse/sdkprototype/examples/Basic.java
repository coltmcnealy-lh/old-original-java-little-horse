package little.horse.sdkprototype.examples;

import little.horse.common.LHConfig;
import little.horse.sdkprototype.LHTaskFunction;
import little.horse.sdkprototype.LHTaskOutput;
import little.horse.sdkprototype.LHVariable;
import little.horse.sdkprototype.sdk.SpecBuilderThreadContext;

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
