package com.github.anicolaspp.ojai;

import org.ojai.Document;
import org.ojai.exceptions.DecodingException;
import org.ojai.store.Connection;

import java.util.Arrays;
import java.util.Optional;

public class LinearProjector implements PathProjector {
    private Document document;
    private Connection connection;
    
    public LinearProjector(Document document, Connection connection) {
        
        this.document = document;
        this.connection = connection;
    }
    
    public Document projectPath(String path) {
        String[] pathSegments = path.replace("\"", "").split("\\.");
        String firstSegment = pathSegments[0];
        
        if (isLastSegment(pathSegments)) {
            return getFinalDocument(firstSegment);
        } else {
            return tryGetSubDoc(firstSegment, document)
                    .map(subDoc -> {
                        Document embedded = new DocumentProjector(connection.newDocument(document.getValue(firstSegment)), connection)
                                .projectPath(getNextSegments(pathSegments));
                        
                        return connection.newDocument().set(firstSegment, embedded);
                    })
                    .orElse(connection.newDocument());
        }
    }
    
    private boolean isLastSegment(String[] pathSegments) {
        return Arrays.stream(pathSegments).skip(1).reduce("", (a, b) -> a + b).isEmpty();
    }
    
    private String getNextSegments(String[] pathSegments) {
        return Arrays.stream(pathSegments).skip(1).reduce("", (a, b) -> a + b);
    }
    
    private Document getFinalDocument(String firstSegment) {
        try {
            return connection.newDocument().set(firstSegment, document.getValue(firstSegment));
        } catch (NullPointerException e) {
            return connection.newDocument();
        }
    }
    
    private Optional<Document> tryGetSubDoc(String field, Document document) {
        try {
            Document subDoc = connection.newDocument(document.getValue(field));
            
            return Optional.ofNullable(subDoc);
        } catch (DecodingException ex) {
            return Optional.empty();
        }
    }
}
