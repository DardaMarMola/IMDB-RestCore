package com.lobox.imdb.api.controller;

import com.lobox.imdb.api.model.Title;
import com.lobox.imdb.api.service.HttpRequestCounterService;
import com.lobox.imdb.api.service.ImdbService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/imdb")
public class ImdbController {

    private final ImdbService imdbService;
    private final HttpRequestCounterService requestCounterService;

    public ImdbController(ImdbService imdbService, HttpRequestCounterService requestCounterService) {
        this.imdbService = imdbService;
        this.requestCounterService = requestCounterService;
    }

    @GetMapping("/titles/same-director-writer-alive")
    public ResponseEntity<List<Title>> getTitlesBySameDirectorWriterAndAlive() {
        List<Title> titles = imdbService.getTitlesBySameDirectorWriterAndAlive();
        return ResponseEntity.ok(titles);
    }

    @GetMapping("/titles/common-actors")
    public ResponseEntity<List<Title>> getTitlesByTwoActors(@RequestParam("actor1Id") String actor1Id, @RequestParam("actor2Id") String actor2Id) {
        List<Title> titles = imdbService.getTitlesByTwoActors(actor1Id, actor2Id);
        if (titles.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(titles);
    }

    @GetMapping("/titles/best-by-genre")
    public ResponseEntity<Map<Integer, Title>> getBestTitlesByGenre(@RequestParam("genre") String genre) {
        Map<Integer, Title> bestTitles = imdbService.getBestTitlesByGenreAndYear(genre);
        if (bestTitles.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(bestTitles);
    }

    @GetMapping("/metrics/http-requests-count")
    public ResponseEntity<Long> getHttpRequestCount() {
        return ResponseEntity.ok(requestCounterService.getCount());
    }
}