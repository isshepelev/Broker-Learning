package manufacture.ru.brokerlearning.repository;

import manufacture.ru.brokerlearning.model.UserResource;
import org.springframework.data.jpa.repository.JpaRepository;

import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface UserResourceRepository extends JpaRepository<UserResource, Long> {

    List<UserResource> findByOwnerSid(String ownerSid);

    List<UserResource> findByResourceTypeAndOwnerSid(String resourceType, String ownerSid);

    Optional<UserResource> findByResourceTypeAndResourceName(String resourceType, String resourceName);

    @Transactional
    void deleteByResourceTypeAndResourceName(String resourceType, String resourceName);

    @Transactional
    void deleteByOwnerSid(String ownerSid);

    default Set<String> topicNamesForUser(String ownerSid) {
        return findByResourceTypeAndOwnerSid("TOPIC", ownerSid).stream()
                .map(UserResource::getResourceName)
                .collect(Collectors.toSet());
    }

    default Set<String> groupNamesForUser(String ownerSid) {
        return findByResourceTypeAndOwnerSid("CONSUMER_GROUP", ownerSid).stream()
                .map(UserResource::getResourceName)
                .collect(Collectors.toSet());
    }
}
