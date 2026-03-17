package manufacture.ru.brokerlearning.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import manufacture.ru.brokerlearning.model.AppUser;
import manufacture.ru.brokerlearning.repository.AppUserRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@Slf4j
public class AppUserService implements UserDetailsService {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Value("${app.admin.username}")
    private String adminUsername;

    @Value("${app.admin.password}")
    private String adminPassword;

    public AppUserService(AppUserRepository userRepository, PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
    }

    @PostConstruct
    public void ensureAdmin() {
        if (!userRepository.existsByUsername(adminUsername)) {
            AppUser admin = AppUser.builder()
                    .username(adminUsername)
                    .password(passwordEncoder.encode(adminPassword))
                    .sid("admin")  // специальный sid для админа — топики без суффикса
                    .role("ROLE_ADMIN")
                    .build();
            userRepository.save(admin);
            log.info("Admin user '{}' created", adminUsername);
        }
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        AppUser user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException("Пользователь не найден: " + username));
        return new User(user.getUsername(), user.getPassword(),
                List.of(new SimpleGrantedAuthority(user.getRole())));
    }

    public AppUser register(String username, String rawPassword) {
        if (userRepository.existsByUsername(username)) {
            throw new IllegalArgumentException("Пользователь '" + username + "' уже существует");
        }
        String sid = UUID.randomUUID().toString().substring(0, 8);
        AppUser user = AppUser.builder()
                .username(username)
                .password(passwordEncoder.encode(rawPassword))
                .sid(sid)
                .role("ROLE_USER")
                .build();
        return userRepository.save(user);
    }

    public List<AppUser> getAllUsers() {
        return userRepository.findAll();
    }

    public String getSid(String username) {
        return userRepository.findByUsername(username)
                .map(AppUser::getSid)
                .orElse("_anonymous");
    }
}
