package com.github.anicolaspp.ojai;

import org.ojai.Document;
import org.ojai.store.Connection;

public class DocumentProjector implements PathProjector {
    
    private Document document;
    private Connection connection;
    
    public DocumentProjector(Document document, Connection connection) {
        
        this.document = document;
        this.connection = connection;
    }
    
    public Document projectPath(String path) {
        
        if (document == null) {
            return connection.newDocument();
        }
        
        if (isEmpty(path)) {
            return document;
        }
        
        if (needsListProjector(path)) {
            return new ListProjector(document, connection).projectPath(path);
            
        } else {
            return new LinearProjector(document, connection).projectPath(path);
        }
    }
    
    private boolean isEmpty(String path) {
        String[] segments = path.replace("\"", "").split("\\.");
        
        return segments.length == 0;
    }
    
    private boolean needsListProjector(String path) {
        String[] segments = path.replace("\"", "").split("\\.");
        
        String first = segments[0];
        
        return first.contains("[]");
    }
}
