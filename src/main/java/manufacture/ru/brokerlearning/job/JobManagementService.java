package manufacture.ru.brokerlearning.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobManagementService {

    private final ScheduledProducerJob scheduledProducerJob;
    private final ScheduledConsumerJob scheduledConsumerJob;

    public void startProducerJob(String sid) { scheduledProducerJob.start(sid); }
    public void stopProducerJob(String sid) { scheduledProducerJob.stop(sid); }
    public boolean isProducerJobRunning(String sid) { return scheduledProducerJob.isRunning(sid); }

    public void startConsumerJob(String sid) { scheduledConsumerJob.start(sid); }
    public void stopConsumerJob(String sid) { scheduledConsumerJob.stop(sid); }
    public boolean isConsumerJobRunning(String sid) { return scheduledConsumerJob.isRunning(sid); }

    public Map<String, Object> getJobStatuses(String sid) {
        Map<String, Object> statuses = new HashMap<>();

        Map<String, Object> producerStatus = new HashMap<>();
        producerStatus.put("running", scheduledProducerJob.isRunning(sid));
        producerStatus.put("messagesProduced", scheduledProducerJob.getCounter(sid));
        producerStatus.put("recentMessages", scheduledProducerJob.getRecentMessages(sid));

        Map<String, Object> consumerStatus = new HashMap<>();
        consumerStatus.put("running", scheduledConsumerJob.isRunning(sid));
        consumerStatus.put("messagesConsumed", scheduledConsumerJob.getCounter(sid));
        consumerStatus.put("recentMessages", scheduledConsumerJob.getRecentMessages(sid));

        statuses.put("producerJob", producerStatus);
        statuses.put("consumerJob", consumerStatus);
        return statuses;
    }

    public void cleanup(String sid) {
        scheduledProducerJob.cleanup(sid);
        scheduledConsumerJob.cleanup(sid);
    }
}
