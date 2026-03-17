package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.config.UserSessionHelper;
import manufacture.ru.brokerlearning.delivery.AtLeastOnceService;
import manufacture.ru.brokerlearning.delivery.AtMostOnceService;
import manufacture.ru.brokerlearning.delivery.ExactlyOnceService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/delivery-semantics")
@Slf4j
@RequiredArgsConstructor
public class DeliverySemanticsController {

    private final AtMostOnceService atMostOnceService;
    private final AtLeastOnceService atLeastOnceService;
    private final ExactlyOnceService exactlyOnceService;
    private final UserSessionHelper sessionHelper;

    @GetMapping("")
    public String deliverySemanticsPage(Model model) {
        model.addAttribute("currentPage", "delivery-semantics");
        return "delivery-semantics";
    }

    @PostMapping("/at-most-once")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> demonstrateAtMostOnce(@RequestParam(defaultValue = "10") int count) {
        Map<String, Object> response = new HashMap<>();
        try {
            response.put("success", true);
            response.putAll(atMostOnceService.demonstrate(sessionHelper.currentSid(), count));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Demonstration failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/at-least-once")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> demonstrateAtLeastOnce(@RequestParam(defaultValue = "10") int count) {
        Map<String, Object> response = new HashMap<>();
        try {
            response.put("success", true);
            response.putAll(atLeastOnceService.demonstrate(sessionHelper.currentSid(), count));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Demonstration failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/exactly-once")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> demonstrateExactlyOnce(@RequestParam(defaultValue = "10") int count) {
        Map<String, Object> response = new HashMap<>();
        try {
            response.put("success", true);
            response.putAll(exactlyOnceService.demonstrate(sessionHelper.currentSid(), count));
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Demonstration failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
