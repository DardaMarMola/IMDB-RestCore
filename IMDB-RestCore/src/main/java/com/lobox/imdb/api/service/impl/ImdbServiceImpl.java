package com.lobox.imdb.api.service.impl;

import com.lobox.imdb.api.model.Person;
import com.lobox.imdb.api.model.Title;
import com.lobox.imdb.api.service.DataLoaderService;
import com.lobox.imdb.api.service.ImdbService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ImdbServiceImpl implements ImdbService {

    private final DataLoaderService dataLoaderService;

    public ImdbServiceImpl(DataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }
                    
    @Override
    public List<Title> getTitlesBySameDirectorWriterAndAlive() {
        log.info("Fetching titles by same director/writer and alive.");
        Map<String, Title> allTitles = dataLoaderService.getTitles();
        Map<String, Person> allPersons = dataLoaderService.getPersons();
        return allTitles.values().stream().filter(title -> title.getDirectorNconsts() != null
                && !title.getDirectorNconsts().isEmpty()).filter(title -> title.getWriterNconsts() != null
                && !title.getWriterNconsts().isEmpty()).filter(title -> {
            if (title.getDirectorNconsts().size() == 1 && title.getWriterNconsts().size() == 1) {
                String directorId = title.getDirectorNconsts().getFirst();
                String writerId = title.getWriterNconsts().getFirst();

                if (directorId.equals(writerId)) {
                    Person person = allPersons.get(directorId);
                    return person != null && person.getDeathYear() == 0;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    @Override
    public List<Title> getTitlesByTwoActors(String actor1Nconst, String actor2Nconst) {
        log.info("Fetching titles where actors {} and {} both played.", actor1Nconst, actor2Nconst);
        Map<String, Title> allTitles = dataLoaderService.getTitles();
        Map<String, Set<String>> titleActorsMap = dataLoaderService.getTitleActors();

        if (!dataLoaderService.getPersons().containsKey(actor1Nconst) || !dataLoaderService.getPersons().containsKey(actor2Nconst)) {
            log.warn("One or both actor IDs not found: {}, {}", actor1Nconst, actor2Nconst);
            return Collections.emptyList();
        }

        return allTitles.values().stream().filter(title -> {
            Set<String> actorsInTitle = titleActorsMap.get(title.getTconst());
            return actorsInTitle != null && actorsInTitle.contains(actor1Nconst) && actorsInTitle.contains(actor2Nconst);
        }).collect(Collectors.toList());
    }

    @Override
    public Map<Integer, Title> getBestTitlesByGenreAndYear(String genre) {
        log.info("Fetching best titles for genre: {}", genre);
        Map<String, Title> allTitles = dataLoaderService.getTitles();
        return allTitles.values().stream().filter(title -> title.getGenres() != null && title.getGenres()
                .contains(genre)).filter(title -> title.getStartYear() != 0 && title.getNumVotes() != 0 &&
                title.getAverageRating() != 0).collect
                (Collectors.groupingBy(Title::getStartYear, Collectors.collectingAndThen
                        (Collectors.maxBy(Comparator.comparingInt(Title::getNumVotes)
                                .thenComparingDouble(Title::getAverageRating))
                                , Optional::orElseThrow)));
    }
}