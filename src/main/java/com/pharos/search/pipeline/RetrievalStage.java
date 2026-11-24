package com.pharos.search.pipeline;

import com.pharos.search.SearchRequest;
import com.pharos.search.SearchResult;
import com.pharos.search.SearchTrace;
import com.pharos.search.distributed.RawCandidate;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.List;

public interface RetrievalStage {
    List<SearchResult> retrieve(IndexReader reader, SearchRequest req, SearchTrace trace)
            throws IOException;

    /**
     * Retrieval without the shaping second pass ({@link com.pharos.search.distributed.CandidateShaper}):
     * unboosted scores plus the shaping inputs, scored with {@code searcher}'s term and collection
     * statistics. Stages may set the similarity they score with on {@code searcher}: use one
     * searcher per call. Only the keyword, vector and unified stages support it.
     */
    default List<RawCandidate> retrieveRaw(IndexSearcher searcher, SearchRequest req, SearchTrace trace)
            throws IOException {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no raw retrieval");
    }
}
