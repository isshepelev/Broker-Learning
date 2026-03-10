package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import manufacture.ru.brokerlearning.service.RealtimeMessageService;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import manufacture.ru.brokerlearning.model.KafkaMessageEntity;

import java.util.List;

@Controller
@RequestMapping("/consumer")
@Slf4j
@RequiredArgsConstructor
public class ConsumerController {

    private final MessageHistoryService historyService;
    private final RealtimeMessageService realtimeMessageService;

    @GetMapping("")
    public String consumerPage(Model model) {
        model.addAttribute("messages", historyService.getMessagesByDirection("RECEIVED"));
        model.addAttribute("currentPage", "consumer");
        return "consumer";
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @ResponseBody
    public SseEmitter stream() {
        return realtimeMessageService.subscribe();
    }

    @GetMapping("/messages")
    @ResponseBody
    public List<KafkaMessageEntity> getRecentMessages() {
        return historyService.getMessagesByDirection("RECEIVED");
    }
}
