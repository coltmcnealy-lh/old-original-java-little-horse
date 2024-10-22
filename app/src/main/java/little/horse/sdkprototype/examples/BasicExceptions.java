package little.horse.sdkprototype.examples;

import little.horse.common.DepInjContext;
import little.horse.sdkprototype.LHTaskFunction;
import little.horse.sdkprototype.sdk.SpecBuilderThreadContext;

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

        wf.execute(
            new TaskThatFails()
        ).doExcept((thread) -> {
            thread.execute(new MyOtherTask(), "some input");

        });
        
        /* 
        Once we get this result working properly, we will add the ability to
        execute multiple tasks in the same thread. We will have to add Edges.
        */
        // thread.execute(new MyOtherTask());

        System.out.println(wf.compile().toString());
    }
}

/* // Old stuff that doesn't work ):
        wf.doTry((w) -> {
            w.executeTask(new TaskThatFails());

        }).doExcept((w) -> {
            w.executeTask(new MyOtherTask());

        });
        */