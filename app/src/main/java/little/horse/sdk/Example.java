package little.horse.sdk;

import little.horse.common.DepInjContext;
import little.horse.sdk.sdk.SpecBuilderThreadContext;

class MyTask {
    @LHTaskFunction
    public Integer doTask(String input) {
        return input.length();
    }
}

public class Example {
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

        System.out.println(wf.compile().toString());
    }
}
