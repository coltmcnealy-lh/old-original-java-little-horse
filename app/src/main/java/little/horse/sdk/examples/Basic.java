package little.horse.sdk.examples;

import little.horse.common.DepInjContext;
import little.horse.sdk.LHTaskFunction;
import little.horse.sdk.LHTaskOutput;
import little.horse.sdk.LHVariable;
import little.horse.sdk.sdk.SpecBuilderThreadContext;

class MyTask {
    @LHTaskFunction
    public Integer doTask(String input) {
        return input.length();
    }
}

public class Basic {
    public static void main(String[] args) {
        SpecBuilderThreadContext wf = new SpecBuilderThreadContext(
            new DepInjContext(), "my-wf"
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
