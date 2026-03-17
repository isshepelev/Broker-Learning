package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.service.MessageHistoryService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/history")
@Slf4j
@RequiredArgsConstructor
public class HistoryController {

    private final MessageHistoryService historyService;
    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String historyPage(Model model) {
        model.addAttribute("messages", historyService.getRecentMessages(sessionHelper.currentSid()));
        model.addAttribute("currentPage", "history");
        return "history";
    }
}
