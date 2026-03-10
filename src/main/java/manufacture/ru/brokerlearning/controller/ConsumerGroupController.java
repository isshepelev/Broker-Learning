package manufacture.ru.brokerlearning.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/consumer-groups")
@Slf4j
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final AdminClient adminClient;

    @GetMapping("")
    public String consumerGroupsPage(Model model) {
        try {
            List<String> groupIds = adminClient.listConsumerGroups().all().get()
                    .stream()
                    .map(listing -> listing.groupId())
                    .collect(Collectors.toList());
            model.addAttribute("consumerGroups", groupIds);
        } catch (Exception e) {
            log.error("Failed to list consumer groups: {}", e.getMessage(), e);
            model.addAttribute("consumerGroups", Collections.emptyList());
        }
        model.addAttribute("currentPage", "consumer-groups");
        return "consumer-groups";
    }
}
