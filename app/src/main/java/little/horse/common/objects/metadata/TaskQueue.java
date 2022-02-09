package little.horse.common.objects.metadata;

import org.apache.kafka.clients.admin.NewTopic;

import little.horse.common.objects.DigestIgnore;
import little.horse.common.util.LHUtil;

public class TaskQueue extends CoreMetadata {
    public static String typeName = "taskQueue";

    @DigestIgnore
    public int partitions = config.getDefaultPartitions();

    // This should be the only thing used in the digest.
    public String kafkaTopic = this.name;

    public void processChange(CoreMetadata old) {
        if (!(old == null || old instanceof TaskQueue)) {
            throw new RuntimeException("whoever made this call is a nincompoop");
        }

        TaskQueue oldTQ = (TaskQueue) old;

        if (oldTQ != null) {
            if (oldTQ.partitions != partitions) {
                LHUtil.logError(
                    "Can't update task queue that already exists!"
                );
            }
        } else {
            config.createKafkaTopic(new NewTopic(
                name, partitions, (short) config.getDefaultReplicas()
            ));
        }
    }
}
