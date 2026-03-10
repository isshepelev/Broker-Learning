package manufacture.ru.brokerlearning.job;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class JobManagementService {

    private final ScheduledProducerJob scheduledProducerJob;
    private final ScheduledConsumerJob scheduledConsumerJob;

    public void startProducerJob() {
        scheduledProducerJob.start();
    }

    public void stopProducerJob() {
        scheduledProducerJob.stop();
    }

    public boolean isProducerJobRunning() {
        return scheduledProducerJob.isRunning();
    }

    public void startConsumerJob() {
        scheduledConsumerJob.start();
    }

    public void stopConsumerJob() {
        scheduledConsumerJob.stop();
    }

    public boolean isConsumerJobRunning() {
        return scheduledConsumerJob.isRunning();
    }

    public Map<String, Object> getJobStatuses() {
        Map<String, Object> statuses = new HashMap<>();

        Map<String, Object> producerStatus = new HashMap<>();
        producerStatus.put("running", scheduledProducerJob.isRunning());
        producerStatus.put("messagesProduced", scheduledProducerJob.getCounter());
        producerStatus.put("recentMessages", scheduledProducerJob.getRecentMessages());

        Map<String, Object> consumerStatus = new HashMap<>();
        consumerStatus.put("running", scheduledConsumerJob.isRunning());
        consumerStatus.put("messagesConsumed", scheduledConsumerJob.getCounter());
        consumerStatus.put("recentMessages", scheduledConsumerJob.getRecentMessages());

        statuses.put("producerJob", producerStatus);
        statuses.put("consumerJob", consumerStatus);

        return statuses;
    }
}
