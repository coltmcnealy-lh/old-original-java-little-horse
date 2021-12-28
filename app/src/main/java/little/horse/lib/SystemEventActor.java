package little.horse.lib;

import little.horse.lib.objects.ExternalEventDef;
import little.horse.lib.objects.WFSpec;
import little.horse.lib.schemas.BaseSchema;
import little.horse.lib.schemas.ExternalEventPayloadSchema;
import little.horse.lib.schemas.NodeSchema;
import little.horse.lib.schemas.WFEventSchema;
import little.horse.lib.schemas.WFRunSchema;
import little.horse.lib.schemas.WFTriggerSchema;

/**
 * SystemEventActor is the Actor called by the WFRuntime KafkaStreams Topology
 * for every WFEvent *On the LH API SERVER*. Essentially, this Actor is
 * responsible for implementing the side effects of any system-level Nodes in
 * the WFSpec.
 *
 * As of this commit, I'm working on adding the processing for External Events; i.e.
 * this class will act as the implementation for the EXTERNAL_EVENT type node. In
 * the future, this class will also service things like time.sleep(), timeouts,
 * spawning subprocesses, waiting for threads to join, etc.
 */
public class SystemEventActor implements WFEventProcessorActor {
    private WFSpec wfSpec;
    private NodeSchema node;
    private Config config;
    private WFTriggerSchema trigger;
    private ExternalEventDef taskDef;

    public void act(WFRunSchema wfRun, WFEventSchema event) {
        if (event.type == WFEventType.EXTERNAL_EVENT) {
            processExternalEvent(wfRun, event);
        } else if (event.type == WFEventType.NODE_COMPLETED) {
            processNodeCompleted(wfRun, event);
        }
    }

    private void processExternalEvent(WFRunSchema wfRun, WFEventSchema event) {
        ExternalEventPayloadSchema payload = BaseSchema.fromString(
            event.content,
            ExternalEventPayloadSchema.class
        );

        if (!payload.externalEventDefGuid.equals(node.externalEventDefGuid)) {
            return;
        }

        
    }

    private void processNodeCompleted(WFRunSchema wfRun, WFEventSchema event) {
    
    }
}
