package com.mapr.db.impl;

import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.store.Connection;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ListProjector implements PathProjector {
    
    private Document document;
    private Connection connection;
    
    ListProjector(Document document, Connection connection) {
        
        this.document = document;
        this.connection = connection;
    }
    
    private Optional<List<Object>> tryGetList(String currentSegment) {
        List<Object> xs = document.getList(FieldPath.parseFrom(currentSegment));
        
        return Optional.ofNullable(xs);
    }
    
    public Document projectPath(String path) {
        String[] pathSegments = path.replace("\"", "").split("\\.");
        String firstSegment = pathSegments[0].replace("[]", "");
        
        return tryGetList(firstSegment)
                .map(Collection::stream)
                .map(stream -> {
                    List<Document> result = stream
                            .map(d -> connection.newDocument(d))
                            .map(d -> {
                                
                                String rest = path.substring(firstSegment.length() + 1);
                                
                                if (!rest.isEmpty()) {
                                    return new DocumentProjector(d, connection).projectPath(rest);
                                    
                                } else {
                                    return d;
                                }
                                
                            })
                            .filter(d -> !d.isEmpty())
                            .collect(Collectors.toList());
                    
                    if (result.size() == 0) {
                        return connection.newDocument();
                    } else {
                        return connection.newDocument().set(firstSegment, result);
                    }
                })
                .orElse(connection.newDocument());
    }
}

