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

    public void startProducerJob() {
        scheduledProducerJob.start();
        log.info("Producer job started via management service");
    }

    public void stopProducerJob() {
        scheduledProducerJob.stop();
        log.info("Producer job stopped via management service");
    }

    public boolean isProducerJobRunning() {
        return scheduledProducerJob.isRunning();
    }

    public void startConsumerJob() {
        scheduledConsumerJob.start();
        log.info("Consumer job started via management service");
    }

    public void stopConsumerJob() {
        scheduledConsumerJob.stop();
        log.info("Consumer job stopped via management service");
    }

    public boolean isConsumerJobRunning() {
        return scheduledConsumerJob.isRunning();
    }

    public Map<String, Object> getJobStatuses() {
        Map<String, Object> statuses = new HashMap<>();

        Map<String, Object> producerStatus = new HashMap<>();
        producerStatus.put("running", scheduledProducerJob.isRunning());
        producerStatus.put("messagesProduced", scheduledProducerJob.getCounter());

        Map<String, Object> consumerStatus = new HashMap<>();
        consumerStatus.put("running", scheduledConsumerJob.isRunning());
        consumerStatus.put("lastMessagesCount", scheduledConsumerJob.getLastMessages().size());

        statuses.put("producerJob", producerStatus);
        statuses.put("consumerJob", consumerStatus);

        return statuses;
    }
}
