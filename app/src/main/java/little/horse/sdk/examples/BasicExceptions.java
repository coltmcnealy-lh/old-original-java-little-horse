package little.horse.sdk.examples;

import little.horse.common.DepInjContext;
import little.horse.sdk.LHTaskFunction;
import little.horse.sdk.sdk.SpecBuilderThreadContext;

class TaskThatFails {
    @LHTaskFunction
    public String doTaskThatFails() {
        throw new RuntimeException("The task just fails!");
    }
}

class MyOtherTask {
    @LHTaskFunction
    public Integer doTask(String input) {
        return input.length();
    }
}

public class BasicExceptions {
    public static void main(String[] args) {
        SpecBuilderThreadContext wf = new SpecBuilderThreadContext(
            new DepInjContext(), "basic-exceptions-sample"
        );

        // The doTry() and doExcept() functions haven't been implemented yet!

        /*
        wf.doTry((w) -> {
            w.executeTask(new TaskThatFails());

        }).doExcept((w) -> {
            w.executeTask(new MyOtherTask());

        });
        */

        wf.execute(
                new TaskThatFails()
        );
        wf.execute(
                new MyOtherTask(),
                "this is the input to the first execution"
        );

        System.out.println(wf.compile().toString());
    }
}
