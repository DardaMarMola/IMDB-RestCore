package com.lobox.imdb.api.service;

import com.lobox.imdb.api.model.Title;

import java.util.List;
import java.util.Map;

public interface ImdbService {

    List<Title> getTitlesBySameDirectorWriterAndAlive();

    List<Title> getTitlesByTwoActors(String actor1Nconst, String actor2Nconst);

    Map<Integer, Title> getBestTitlesByGenreAndYear(String genre);
}
