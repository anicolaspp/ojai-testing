package com.mapr.db.impl;

import org.ojai.Document;
import org.ojai.exceptions.DecodingException;
import org.ojai.store.Connection;

import java.util.Arrays;

public class LinearProjector implements PathProjector {
    private Document document;
    private Connection connection;
    
    LinearProjector(Document document, Connection connection) {
        
        this.document = document;
        this.connection = connection;
    }
    
    public Document projectPath(String path) {
        String[] pathSegments = path.replace("\"", "").split("\\.");
        String firstSegment = pathSegments[0];
        
        String rest = Arrays.stream(pathSegments).skip(1).reduce("", (a, b) -> a + b);
        
        if (!rest.isEmpty()) {
            
            Document subDoc = tryGetSubDoc(firstSegment, document);
            
            if (subDoc == null) {
                return connection.newDocument();
            }
            
            return connection
                    .newDocument()
                    .set(firstSegment, new DocumentProjector(connection.newDocument(document.getValue(firstSegment)), connection).projectPath(rest));
            
        } else {
            try {
                return connection.newDocument().set(firstSegment, document.getValue(firstSegment));
            } catch (NullPointerException e) {
                return connection.newDocument();
            }
            
        }
    }
    
    private Document tryGetSubDoc(String field, Document document) {
        try {
            return connection.newDocument(document.getValue(field));
        } catch (DecodingException ex) {
            return connection.newDocument();
        }
    }
}
