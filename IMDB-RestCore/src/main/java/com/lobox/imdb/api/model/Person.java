package com.lobox.imdb.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String nconst;
    private String primaryName;
    private Integer birthYear;
    private Integer deathYear;
    private String primaryProfession;
    private List<String> knownForTitles;
}