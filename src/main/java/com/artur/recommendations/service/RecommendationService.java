package com.artur.recommendations.service;

import com.artur.common.entity.VideoEntity;
import com.artur.common.exception.NotFoundException;
import com.artur.common.repository.LikeRepository;
import com.artur.common.repository.UserRepository;
import com.artur.recommendations.repository.RecommendationsRepository;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RecommendationService {
    private static final Logger logger = LoggerFactory.getLogger(RecommendationService.class);

    @Autowired
    UserRepository userRepository;
    @Autowired
    RecommendationsRepository recommendationsRepository;
    @Value("${application.max-videos-per-request:15}")
    Integer maxVideosPerRequest;
    @Value("${application.popularity-days:3}")
    Integer popularityDays;


    /** Gets recommendations. Videos will not be repeated and will not be contained in {@code excludes}. <p>
     * The main idea is to get {@code  AppConstants.RECS_SIZE} amount of videos in <strong>one SQL request</strong>.
     * This SQL request explained in {@link LikeRepository}.findRecommendations() method.<p>
     *  There two ways to get recommendations:
     *  <ul>
     *      <li>
     *          If user id is not null it gets his categories points and returns the most popular videos
     *          with them and with the users most common user languages. If after one SQL request amount of videos is less
     *          than {@code RECS_SIZE} (it can be if user don`t have enough metadata or number of existed videos is to small),
     *          it will find the most popular videos with the specified browser languages for any topic. If still have not enough
     *          videos, it will find just some popular videos without any language, that's should not be happened if we have enough
     *          videos in database.
     *      </li>
     *      <li>
     *          If user id is null, it will return recommendations with specified languages. This languages should be taken from
     *          browser or found by user`s country language which can be obtained by ip address.
     *      </li>
     *  </ul>
     *  The most popular videos are those that have the most likes during {@code Instant.now().minus(MAX_POPULARITY_DAYS)}.
     *
     * @param userId user id for which should be recommendations found. Can be null.
     * @param page page.
     * @param browserLanguages browser languages from which the request was made. Can not be null and empty
     * @param size the size of recommendations
     * @return Ids of founded recommendations
     * @throws NotFoundException if user id is not null, but user with this id is not found
     */
    public List<Long> getRecommendationsFor(
            @Nullable String userId,
            @NotNull Integer page,
            @NotEmpty String[] browserLanguages,
            @NotNull Integer size) throws NotFoundException {
        long start = System.currentTimeMillis();
        final int RECS_SIZE = Math.min(size, maxVideosPerRequest);
        Set<Long> ids = new HashSet<>();
        if(userId != null){
            if(!userRepository.existsById(userId))
                throw new NotFoundException("User with specified id [" + userId + "] was not found");
            ids.addAll(getRecommendationsForUser(userId, page, RECS_SIZE));
        }

        if(ids.size() < RECS_SIZE){
            //finding with browser language
            ids.addAll(getByLanguages(
                    ids.isEmpty() ? Set.of(-1L) : ids,
                    page,
                    browserLanguages, RECS_SIZE - ids.size()));
        }
        //finding random popular ids
        if(ids.size() < RECS_SIZE){
            logger.warn("Recommendation not found with user and browser languages for user: " + userId);
            ids.addAll(getSomePopularVideos(
                    ids.isEmpty() ? Set.of(-1L) : ids,
                    page,
                    RECS_SIZE - ids.size()));
        }
        if(ids.size() < RECS_SIZE){
            logger.warn("Finding just random ids: " + userId);
            ids.addAll(recommendationsRepository.findByIdNotIn(
                    ids.isEmpty() ? Set.of(-1L) : ids,
                    PageRequest.of( page,RECS_SIZE - ids.size())
                    )
                    .stream().map(VideoEntity::getId).collect(Collectors.toSet()));
        }
        var result = new ArrayList<>(ids);
        Collections.shuffle(result);
        logger.trace("Recommendations found in " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }


    /**Gets videos by his categories and languages points. Calls corresponding method from {@code likeRepository}.
     * The most popular videos will be selected by likes which date is in range from {@code Instant.now().minus(AppConstants.POPULARITY_DAYS)}
     * @param userId user id for which recommendations should be found
     * @param size size of recommendations result
     * @return Ids of found recommendations
     */
    private List<Long> getRecommendationsForUser(String userId, int page, int size){
        return recommendationsRepository.findRecommendationsForUser(userId,
                Instant.now().minus(popularityDays, ChronoUnit.DAYS),
                PageRequest.of(page, size));
    }


    /**Gets recommendations just by languages. For example if user id is unknown, we can find recommendations
     * by user`s browser languages. This languages should be ordered by priority. Firstly it will found
     * videos by first language and if still not enough videos it will continue to next language and so on.
     * @param exceptions videos ids that should be excluded. Can be null or empty.
     * @param languages languages, for example {"ru", "en"}. Can not be null and empty
     * @param size size of recommendations result
     * @return Ids of found recommendations
     */
    private List<Long> getByLanguages(Set<Long> exceptions, int page, String[] languages, int size){
        List<Long> result = new ArrayList<>(size);
        for (String language:languages) {
              result.addAll(getSome(
                      language,
                      exceptions,
                      page,
                      size - result.size()));
              if(result.size() == size) break;
        }
        return result;
    }

    /**Gets recommendations by one language. The most popular videos will be selected by likes which date
     * is in range from {@code Instant.now().minus(AppConstants.POPULARITY_DAYS)}.
     * @param language language
     * @param exceptions videos ids that should be excluded. Can be null or empty.
     * @param size size of recommendations result
     * @return Ids of found recommendations
     */
    private List<Long> getSome(String language, Set<Long> exceptions, int page, int size){
        return recommendationsRepository.findMostPopularVideos(
                Instant.now().minus(popularityDays, ChronoUnit.DAYS),
                language,
                exceptions,
                PageRequest.of(page, size));
    }

    /**Gets just some popular videos without categories nor languages. The most popular videos will be selected by
     * likes which date is in range from {@code Instant.now().minus(AppConstants.POPULARITY_DAYS)}.
     * @param exceptions videos ids that should be excluded. Can be null or empty.
     * @param size size of recommendations result
     * @return Ids of found recommendations
     */
    private List<Long> getSomePopularVideos(Set<Long> exceptions, int page, int size){
        return recommendationsRepository.findMostPopularVideos(
                Instant.now().minus(popularityDays, ChronoUnit.DAYS),
                exceptions,
                PageRequest.of(page, size)
        );

    }
}
