package manufacture.ru.brokerlearning.service;

import lombok.RequiredArgsConstructor;
import manufacture.ru.brokerlearning.model.AppUser;
import manufacture.ru.brokerlearning.repository.AppUserRepository;
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
@RequiredArgsConstructor
public class AppUserService implements UserDetailsService {

    private final AppUserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        AppUser user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException("Пользователь не найден: " + username));
        return new User(user.getUsername(), user.getPassword(),
                List.of(new SimpleGrantedAuthority("ROLE_USER")));
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
                .build();
        return userRepository.save(user);
    }

    /** Возвращает стабильный sid пользователя по username */
    public String getSid(String username) {
        return userRepository.findByUsername(username)
                .map(AppUser::getSid)
                .orElse("_anonymous");
    }
}
