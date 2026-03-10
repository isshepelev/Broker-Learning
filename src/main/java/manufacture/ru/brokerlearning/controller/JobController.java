package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.job.JobManagementService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/jobs")
@Slf4j
@RequiredArgsConstructor
public class JobController {

    private final JobManagementService jobService;

    @GetMapping("")
    public String jobsPage(Model model) {
        model.addAttribute("statuses", jobService.getJobStatuses());
        model.addAttribute("currentPage", "jobs");
        return "jobs";
    }

    @PostMapping("/producer/start")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> startProducerJob() {
        Map<String, Object> response = new HashMap<>();
        try {
            jobService.startProducerJob();
            response.put("success", true);
            response.put("message", "Producer job started");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to start producer job: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to start producer job: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/producer/stop")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> stopProducerJob() {
        Map<String, Object> response = new HashMap<>();
        try {
            jobService.stopProducerJob();
            response.put("success", true);
            response.put("message", "Producer job stopped");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to stop producer job: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to stop producer job: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/consumer/start")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> startConsumerJob() {
        Map<String, Object> response = new HashMap<>();
        try {
            jobService.startConsumerJob();
            response.put("success", true);
            response.put("message", "Consumer job started");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to start consumer job: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to start consumer job: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/consumer/stop")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> stopConsumerJob() {
        Map<String, Object> response = new HashMap<>();
        try {
            jobService.stopConsumerJob();
            response.put("success", true);
            response.put("message", "Consumer job stopped");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to stop consumer job: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to stop consumer job: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @GetMapping("/status")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getJobStatuses() {
        Map<String, Object> response = new HashMap<>();
        try {
            response.put("success", true);
            response.putAll(jobService.getJobStatuses());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Failed to get job statuses: {}", e.getMessage(), e);
            response.put("success", false);
            response.put("error", "Failed to get job statuses: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}
