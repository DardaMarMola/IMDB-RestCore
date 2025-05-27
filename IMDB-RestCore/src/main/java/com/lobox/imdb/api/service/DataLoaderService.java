package com.lobox.imdb.api.service;

import com.lobox.imdb.api.model.Person;
import com.lobox.imdb.api.model.Title;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.*;

@Service
@Slf4j
@Data
public class DataLoaderService {

    private final Map<String, Person> persons = new ConcurrentHashMap<>(2_000_000);
    private final Map<String, Title> titles = new ConcurrentHashMap<>(10_000_000);
    private final Map<String, List<String>> titleDirectors = new ConcurrentHashMap<>(10_000_000);
    private final Map<String, List<String>> titleWriters = new ConcurrentHashMap<>(10_000_000);
    private final Map<String, Set<String>> titleActors = new ConcurrentHashMap<>(20_000_000);
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    @PostConstruct
    public void loadData() {
        long startTime = System.currentTimeMillis();
        log.info("Starting data loading...");

        try {
            log.info("Submitting loadTitlesBasics task...");
            Future<Void> titlesBasicsFuture = executorService.submit(() -> {
                loadTitlesBasics();
                return null;
            });
            titlesBasicsFuture.get();
            log.info("Finished loading basic titles. Proceeding with dependent tasks...");
            List<Callable<Void>> dependentTasks = List.of(() -> {
                loadPersons();
                return null;
            }, () -> {
                loadTitlesCrew();
                return null;
            }, () -> {
                loadTitlesPrincipals();
                return null;
            }, () -> {
                loadTitlesRatings();
                return null;
            });
            List<Future<Void>> futures = executorService.invokeAll(dependentTasks);
            for (Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Data loading interrupted: {}", e.getMessage(), e);
            throw new RuntimeException("Data loading was interrupted", e);
        } catch (ExecutionException e) {
            log.error("Error during parallel data loading: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Failed to load IMDB data", e.getCause());
        } catch (Exception e) {
            log.error("An unexpected error occurred during data loading: {}", e.getMessage(), e);
            throw new RuntimeException("An unexpected error occurred during IMDB data loading", e);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Executor service did not terminate in the specified time.");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Executor service termination interrupted.", e);
                executorService.shutdownNow();
            }
        }
        log.info("Data loading complete in {} ms. Persons: {}, Titles: {}", System.currentTimeMillis() - startTime, persons.size(), titles.size());
    }

    public InputStream getResourceStream(String path) {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("imdbData/" + path);
        if (inputStream == null) {
            log.error("CRITICAL ERROR: '{}' not found in classpath. Please ensure it's in src/main/resources/imdbData/.", path);
            throw new RuntimeException("Required data file not found: " + path);
        }
        return inputStream;
    }

    public void loadPersons() throws IOException, CsvValidationException {

        log.info("Loading persons (name.basics.tsv)...");
        try (Reader reader = new InputStreamReader(getResourceStream("name.basics.tsv"))) {
            CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();
            String[] line;
            long lineNumber = 1;
            while ((line = csvReader.readNext()) != null) {
                lineNumber++;
                if (line.length >= 6) {
                    try {
                        String birthYearStr = line[2].trim();
                        String deathYearStr = line[3].trim();

                        Integer birthYear = "N".equals(birthYearStr) ? null : Integer.parseInt(birthYearStr);
                        Integer deathYear = "N".equals(deathYearStr) ? null : Integer.parseInt(deathYearStr);
                        List<String> primaryProfessions = "N".equals(line[5].trim()) || line[5].trim().isEmpty() ? Collections.emptyList() : List.of(line[5].split(","));
                        Person person = new Person(line[0], line[1], birthYear, deathYear, line[4], primaryProfessions);
                        persons.put(person.getNconst(), person);
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException on line {} in name.basics.tsv: For input string: '{}'. Full line: {}", lineNumber, e.getMessage().replace("For input string: \"", "").replace("\"", ""), String.join("\t", line));
                    } catch (ArrayIndexOutOfBoundsException e) {
                        log.error("ArrayIndexOutOfBoundsException on line {} in name.basics.tsv: Missing field. Full line: {}", lineNumber, String.join("\t", line));
                    }
                } else {
                    log.warn("Skipping malformed line {} in name.basics.tsv. Expected 6 fields, got {}: {}", lineNumber, line.length, String.join("\t", line));
                }
            }
        }
        log.info("Loaded {} persons.", persons.size());
    }

    private void loadTitlesBasics() throws IOException, CsvValidationException {
        log.info("Loading basic titles (title.basics.tsv)...");
        try (Reader reader = new InputStreamReader(getResourceStream("title.basics.tsv"))) {
            CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();
            String[] line;
            long lineNumber = 1;
            while ((line = csvReader.readNext()) != null) {
                lineNumber++;
                if (line.length >= 9) {
                    try {
                        Title title = new Title();
                        title.setTconst(line[0]);
                        title.setTitleType(line[1]);
                        title.setPrimaryTitle(line[2]);
                        title.setOriginalTitle(line[3]);
                        title.setAdult("1".equals(line[4]));
                        title.setStartYear("N".equals(line[5].trim()) ? 0 : Integer.parseInt(line[5].trim()));
                        title.setEndYear("N".equals(line[6].trim()) ? 0 : Integer.parseInt(line[6].trim()));
                        title.setRuntimeMinutes("N".equals(line[7].trim()) ? 0 : Integer.parseInt(line[7].trim()));
                        title.setGenres("N".equals(line[8].trim()) || line[8].trim().isEmpty() ? Collections.emptyList() : List.of(line[8].split(",")));
                        titles.put(title.getTconst(), title);
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException on line {} in title.basics.tsv: For input string: '{}'. Full line: {}", lineNumber, e.getMessage().replace("For input string: \"", "").replace("\"", ""), String.join("\t", line));
                    } catch (ArrayIndexOutOfBoundsException e) {
                        log.error("ArrayIndexOutOfBoundsException on line {} in title.basics.tsv: Missing field. Full line: {}", lineNumber, String.join("\t", line));
                    }
                } else {
                    log.warn("Skipping malformed line {} in title.basics.tsv. Expected 9 fields, got {}: {}", lineNumber, line.length, String.join("\t", line));
                }
            }
        }
        log.info("Loaded {} basic titles.", titles.size());
    }

    private void loadTitlesCrew() throws IOException, CsvValidationException {
        log.info("Loading title crew (title.crew.tsv)...");
        try (Reader reader = new InputStreamReader(getResourceStream("title.crew.tsv"))) {
            CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();
            String[] line;
            long lineNumber = 1;
            while ((line = csvReader.readNext()) != null) {
                lineNumber++;
                if (line.length >= 3) {
                    String tconst = line[0];
                    Title title = titles.get(tconst);
                    if (title != null) {
                        List<String> directors = "N".equals(line[1].trim()) || line[1].trim().isEmpty() ? Collections.emptyList() : Arrays.asList(line[1].split(","));
                        List<String> writers = "N".equals(line[2].trim()) || line[2].trim().isEmpty() ? Collections.emptyList() : Arrays.asList(line[2].split(","));
                        title.setDirectorNconsts(directors);
                        title.setWriterNconsts(writers);
                        titleDirectors.put(tconst, directors);
                        titleWriters.put(tconst, writers);
                    }
                } else {
                    log.warn("Skipping malformed line {} in title.crew.tsv. Expected 3 fields, got {}: {}", lineNumber, line.length, String.join("\t", line));
                }
            }
        }
        log.info("Loaded title crew information.");
    }

    private void loadTitlesPrincipals() throws IOException, CsvValidationException {
        log.info("Loading title principals (title.principals.tsv)...");
        try (Reader reader = new InputStreamReader(getResourceStream("title.principals.tsv"))) {
            CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();
            String[] line;
            long lineNumber = 1;
            while ((line = csvReader.readNext()) != null) {
                lineNumber++;
                if (line.length >= 3) { // Ensure at least tconst, ordering, nconst are present
                    String tconst = line[0];
                    String nconst = line[2];
                    if (titles.containsKey(tconst)) { // Only add if the title exists
                        // computeIfAbsent is good here for thread-safe concurrent updates
                        titleActors.computeIfAbsent(tconst, k -> ConcurrentHashMap.newKeySet()).add(nconst);
                    }
                } else {
                    log.warn("Skipping malformed line {} in title.principals.tsv. Expected at least 3 fields, got {}: {}", lineNumber, line.length, String.join("\t", line));
                }
            }
        }
        log.info("Loaded title principal information.");
    }

    private void loadTitlesRatings() throws IOException, CsvValidationException {
        log.info("Loading title ratings (title.ratings.tsv)...");
        try (Reader reader = new InputStreamReader(getResourceStream("title.ratings.tsv"))) {
            CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build();
            String[] line;
            long lineNumber = 1;
            while ((line = csvReader.readNext()) != null) {
                lineNumber++;
                if (line.length >= 3) {
                    try {
                        String tconst = line[0];
                        Title title = titles.get(tconst); // Get the already loaded Title object
                        if (title != null) { // Only process if the title exists in our map
                            Double averageRating = "N".equals(line[1].trim()) ? null : Double.parseDouble(line[1].trim());
                            Integer numVotes = "N".equals(line[2].trim()) ? null : Integer.parseInt(line[2].trim());

                            title.setAverageRating(averageRating);
                            title.setNumVotes(numVotes);
                        }
                    } catch (NumberFormatException e) {
                        log.error("NumberFormatException on line {} in title.ratings.tsv: For input string: '{}'. Full line: {}", lineNumber, e.getMessage().replace("For input string: \"", "").replace("\"", ""), String.join("\t", line));
                    } catch (ArrayIndexOutOfBoundsException e) {
                        log.error("ArrayIndexOutOfBoundsException on line {} in title.ratings.tsv: Missing field. Full line: {}", lineNumber, String.join("\t", line));
                    }
                } else {
                    log.warn("Skipping malformed line {} in title.ratings.tsv. Expected 3 fields, got {}: {}", lineNumber, line.length, String.join("\t", line));
                }
            }
        }
        log.info("Loaded title ratings.");
    }
}
