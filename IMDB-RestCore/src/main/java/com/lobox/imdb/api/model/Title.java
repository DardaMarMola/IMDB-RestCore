package com.lobox.imdb.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Title {
    private String tconst;
    private String titleType;
    private String primaryTitle;
    private String originalTitle;
    private boolean isAdult;
    private Integer startYear;
    private Integer endYear;
    private Integer runtimeMinutes;
    private List<String> genres;
    private List<String> directorNconsts;
    private List<String> writerNconsts;
    private Double averageRating;
    private Integer numVotes;
}