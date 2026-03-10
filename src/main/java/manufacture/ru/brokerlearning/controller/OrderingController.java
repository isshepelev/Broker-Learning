package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.service.OrderingService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Controller
@RequestMapping("/ordering")
@RequiredArgsConstructor
public class OrderingController {

    private final OrderingService orderingService;

    @GetMapping("")
    public String page(Model model) {
        model.addAttribute("currentPage", "ordering");
        return "ordering";
    }

    @PostMapping("/run")
    @ResponseBody
    public Map<String, Object> run(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            int count = Integer.parseInt(request.getOrDefault("count", "10").toString());
            count = Math.max(3, Math.min(count, 1000));
            Map<String, Object> result = orderingService.runDemo(count);
            response.put("success", true);
            response.putAll(result);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }

    @PostMapping("/run-multi")
    @ResponseBody
    public Map<String, Object> runMulti(@RequestBody Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            int countPerKey = Integer.parseInt(request.getOrDefault("countPerKey", "5").toString());
            countPerKey = Math.max(2, Math.min(countPerKey, 20));

            @SuppressWarnings("unchecked")
            List<String> keys = (List<String>) request.getOrDefault("keys",
                    List.of("user-A", "user-B", "user-C"));

            Map<String, Object> result = orderingService.runMultiKeyDemo(countPerKey, keys);
            response.put("success", true);
            response.putAll(result);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
        }
        return response;
    }
}
