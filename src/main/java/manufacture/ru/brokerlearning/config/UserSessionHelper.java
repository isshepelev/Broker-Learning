package manufacture.ru.brokerlearning.config;

import manufacture.ru.brokerlearning.model.AppUser;
import manufacture.ru.brokerlearning.repository.AppUserRepository;
import manufacture.ru.brokerlearning.repository.UserResourceRepository;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class UserSessionHelper {

    private final AppUserRepository userRepository;
    private final UserResourceRepository resourceRepository;

    public UserSessionHelper(AppUserRepository userRepository, UserResourceRepository resourceRepository) {
        this.userRepository = userRepository;
        this.resourceRepository = resourceRepository;
    }

    /** Возвращает стабильный sid текущего авторизованного пользователя */
    public String currentSid() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated() || "anonymousUser".equals(auth.getPrincipal())) {
            return "_anonymous";
        }
        String username = auth.getName();
        return userRepository.findByUsername(username)
                .map(AppUser::getSid)
                .orElse("_anonymous");
    }

    /** Возвращает username текущего пользователя */
    public String currentUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) return "anonymous";
        return auth.getName();
    }

    /** Возвращает имена топиков, принадлежащих текущему пользователю */
    public Set<String> currentUserTopics() {
        return resourceRepository.topicNamesForUser(currentSid());
    }

    public static final String ADMIN_SID = "admin";

    /** true если sid принадлежит админу — топики без суффикса */
    public static boolean isAdminSid(String sid) {
        return ADMIN_SID.equals(sid);
    }

    /** Строит имя: prefix для админа, prefix + "-" + sid для обычных пользователей */
    public static String topicName(String prefix, String sid) {
        return isAdminSid(sid) ? prefix : prefix + "-" + sid;
    }

    /** Строит имя consumer group: prefix для админа, prefix + "-" + sid для обычных */
    public static String groupName(String prefix, String sid) {
        return isAdminSid(sid) ? prefix : prefix + "-" + sid;
    }

    public boolean isAdmin() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) return false;
        return auth.getAuthorities().stream()
                .anyMatch(a -> "ROLE_ADMIN".equals(a.getAuthority()));
    }
}
