package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.model.MessageDto;
import manufacture.ru.brokerlearning.producer.CallbackProducer;
import manufacture.ru.brokerlearning.producer.SimpleProducer;
import manufacture.ru.brokerlearning.producer.TransactionalProducer;
import manufacture.ru.brokerlearning.service.KafkaAdminService;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("/producer")
@Slf4j
@RequiredArgsConstructor
public class ProducerController {

    private final SimpleProducer simpleProducer;
    private final CallbackProducer callbackProducer;
    private final TransactionalProducer transactionalProducer;
    private final KafkaAdminService adminService;
    private final MessageHistoryService historyService;
    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String producerPage(Model model) {
        try {
            Set<String> topics = adminService.listTopics();
            topics.retainAll(sessionHelper.currentUserTopics());
            model.addAttribute("topics", topics);
        } catch (Exception e) {
            log.warn("Unable to list topics: {}", e.getMessage());
            model.addAttribute("topics", Collections.emptySet());
        }
        model.addAttribute("currentPage", "producer");
        return "producer";
    }

    @PostMapping("/send")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> sendMessage(@RequestBody MessageDto messageDto) {
        Map<String, Object> response = new HashMap<>();
        try {
            String topic = messageDto.getTopic();
            String key = messageDto.getKey();
            String value = messageDto.getValue();
            String sendMode = messageDto.getSendMode();

            switch (sendMode != null ? sendMode : "") {
                case "fire-and-forget":
                    simpleProducer.sendFireAndForget(topic, key, value);
                    break;
                case "sync":
                    String result = simpleProducer.sendSync(topic, key, value);
                    response.put("sendResult", result);
                    break;
                case "callback":
                    callbackProducer.sendWithCallback(topic, key, value);
                    break;
                case "transactional":
                    transactionalProducer.sendInTransaction(topic, key, value, topic, value);
                    break;
                default:
                    simpleProducer.sendFireAndForget(topic, key, value);
                    break;
            }

            historyService.saveSentMessage(topic, key, value, messageDto.getPartition(),
                    messageDto.getHeaders() != null ? messageDto.getHeaders().toString() : null,
                    sessionHelper.currentSid());

            response.put("success", true);
            response.put("details", "Message sent successfully using mode: " +
                    (sendMode != null ? sendMode : "fire-and-forget"));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to send message: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("details", "Failed to send message: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
