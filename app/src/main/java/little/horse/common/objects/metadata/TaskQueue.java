package little.horse.common.objects.metadata;

import little.horse.common.util.LHUtil;

public class TaskQueue extends CoreMetadata {
    public String foo;

    public void processChange(CoreMetadata old) {
        if (!(old instanceof TaskQueue)) {
            throw new RuntimeException("whoever made this call is a nincompoop");
        }
        TaskQueue oldTQ = (TaskQueue) old;
        LHUtil.log("Processing: ", oldTQ);
        throw new RuntimeException("TODO");
    }
}
