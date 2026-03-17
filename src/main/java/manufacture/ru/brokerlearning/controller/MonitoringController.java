package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.MonitoringService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/monitoring")
@RequiredArgsConstructor
public class MonitoringController {

    private final MonitoringService monitoringService;
    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "monitoring");
        return "monitoring";
    }

    @GetMapping("/data")
    @ResponseBody
    public Map<String, Object> getData() {
        String sid = sessionHelper.currentSid();
        Map<String, Object> data = new HashMap<>();
        data.put("stats", monitoringService.getOverallStats(sid));
        data.put("throughput", monitoringService.getMessageThroughput(sid, 5));
        data.put("consumerLags", monitoringService.getConsumerGroupLags(sid));
        data.put("topicSizes", monitoringService.getTopicSizes(sid));
        return data;
    }
}
