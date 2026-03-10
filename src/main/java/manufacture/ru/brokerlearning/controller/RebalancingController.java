package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.service.RebalancingService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Controller
@RequestMapping("/rebalancing")
@RequiredArgsConstructor
public class RebalancingController {

    private final RebalancingService rebalancingService;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "rebalancing");
        return "rebalancing";
    }

    @PostMapping("/add")
    @ResponseBody
    public Map<String, Object> addConsumer() {
        return rebalancingService.addConsumer();
    }

    @PostMapping("/remove")
    @ResponseBody
    public Map<String, Object> removeConsumer() {
        return rebalancingService.removeConsumer();
    }

    @PostMapping("/reset")
    @ResponseBody
    public Map<String, Object> reset() {
        return rebalancingService.reset();
    }

    @GetMapping("/status")
    @ResponseBody
    public Map<String, Object> status() {
        return rebalancingService.getStatus();
    }
}
