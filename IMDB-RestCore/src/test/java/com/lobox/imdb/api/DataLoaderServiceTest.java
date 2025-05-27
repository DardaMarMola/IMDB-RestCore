package com.lobox.imdb.api;

import com.lobox.imdb.api.service.DataLoaderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;

import static org.junit.jupiter.api.Assertions.*;

class DataLoaderServiceTest {

    private DataLoaderService dataLoaderService;

    @BeforeEach
    void setUp() {
        dataLoaderService = Mockito.spy(new DataLoaderService());
    }

    @Test
    void testLoadData_executesWithoutException() {
        // Mocking the getResourceStream method to return dummy data for each file
        String dummyPersonData = "nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n" + "nm0000001\tFred Astaire\t1899\t1987\tactor,soundtrack,miscellaneous\ttt0072308,tt0050419";

        String dummyTitleData = "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n" + "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short";

        // Mocking each data file
        Mockito.doReturn(new ByteArrayInputStream(dummyPersonData.getBytes())).when(dataLoaderService).getResourceStream("name.basics.tsv");

        Mockito.doReturn(new ByteArrayInputStream(dummyTitleData.getBytes())).when(dataLoaderService).getResourceStream("title.basics.tsv");

        Mockito.doReturn(new ByteArrayInputStream("tconst\tdirectors\twriters\ntt0000001\tnm0000001\tnm0000002".getBytes())).when(dataLoaderService).getResourceStream("title.crew.tsv");

        Mockito.doReturn(new ByteArrayInputStream("tconst\tordering\tnconst\tcategory\tjob\tcharacters\ntt0000001\t1\tnm0000001\tactor\t\t[]".getBytes())).when(dataLoaderService).getResourceStream("title.principals.tsv");

        Mockito.doReturn(new ByteArrayInputStream("tconst\taverageRating\tnumVotes\ntt0000001\t5.6\t1600".getBytes())).when(dataLoaderService).getResourceStream("title.ratings.tsv");

        // Run data loading
        assertDoesNotThrow(() -> dataLoaderService.loadData());

        // Check if data maps are filled as expected
        assertFalse(dataLoaderService.getPersons().isEmpty());
        assertFalse(dataLoaderService.getTitles().isEmpty());
    }

    @Test
    void testMissingResource_throwsException() {
        Mockito.doReturn(null).when(dataLoaderService).getResourceStream("imdbData/name.basics.tsv");
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {dataLoaderService.loadPersons();});
        assertTrue(exception.getMessage().contains("Required data file not found"));
    }
}
