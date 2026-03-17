package manufacture.ru.brokerlearning.config;

import jakarta.servlet.http.HttpSessionEvent;
import jakarta.servlet.http.HttpSessionListener;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.job.JobManagementService;
import manufacture.ru.brokerlearning.model.AppUser;
import manufacture.ru.brokerlearning.repository.AppUserRepository;
import manufacture.ru.brokerlearning.service.*;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SessionCleanupListener implements HttpSessionListener {

    private final ReplayService replayService;
    private final ReplayPracticeService replayPracticeService;
    private final DlqCompareService dlqCompareService;
    private final RebalancingService rebalancingService;
    private final RebalancingPracticeService rebalancingPracticeService;
    private final OrderingService orderingService;
    private final JobManagementService jobManagementService;
    private final AppUserRepository userRepository;

    public SessionCleanupListener(ReplayService replayService,
                                  ReplayPracticeService replayPracticeService,
                                  DlqCompareService dlqCompareService,
                                  RebalancingService rebalancingService,
                                  RebalancingPracticeService rebalancingPracticeService,
                                  OrderingService orderingService,
                                  JobManagementService jobManagementService,
                                  AppUserRepository userRepository) {
        this.replayService = replayService;
        this.replayPracticeService = replayPracticeService;
        this.dlqCompareService = dlqCompareService;
        this.rebalancingService = rebalancingService;
        this.rebalancingPracticeService = rebalancingPracticeService;
        this.orderingService = orderingService;
        this.jobManagementService = jobManagementService;
        this.userRepository = userRepository;
    }

    @Override
    public void sessionDestroyed(HttpSessionEvent se) {
        String sid = null;
        try {
            Object ctx = se.getSession().getAttribute("SPRING_SECURITY_CONTEXT");
            if (ctx instanceof SecurityContext sc && sc.getAuthentication() != null) {
                String username = sc.getAuthentication().getName();
                sid = userRepository.findByUsername(username).map(AppUser::getSid).orElse(null);
            }
        } catch (Exception ignored) {}

        if (sid == null) return;
        log.info("Session destroyed, cleaning up for sid={}", sid);

        try { replayService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { replayPracticeService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { dlqCompareService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { rebalancingService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { rebalancingPracticeService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { orderingService.cleanupSession(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
        try { jobManagementService.cleanup(sid); } catch (Exception e) { log.warn("Cleanup: {}", e.getMessage()); }
    }
}
