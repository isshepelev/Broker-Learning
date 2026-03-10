package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.KafkaMessageEntity;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

@Controller
@RequestMapping("/consumer")
@Slf4j
@RequiredArgsConstructor
public class ConsumerController {

    private final MessageHistoryService historyService;

    @GetMapping("")
    public String consumerPage(Model model) {
        model.addAttribute("currentPage", "consumer");
        return "consumer";
    }

    @GetMapping("/messages")
    @ResponseBody
    public List<KafkaMessageEntity> getRecentMessages() {
        return historyService.getMessagesByDirection("RECEIVED");
    }
}
