package com.mapr.db.impl;

import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.Connection;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiPathProjector {
    
    private final Document document;
    private final Connection connection;
    
    public MultiPathProjector(Document document, Connection connection) {
        
        this.document = document;
        this.connection = connection;
    }
    
    public Document projectPath(String... fieldPaths) {
        if (fieldPaths.length == 0) {
            return document;
        } else {
            Document result = connection.newDocument();
            
            Arrays.stream(fieldPaths)
                    .forEach(field -> {
                        
                        if (field.contains(".")) {
                            
                            String pathToProject = field.replace("\"", "");
                            
                            Document d = new DocumentProjector(document, connection).projectPath(pathToProject);
                            
                            String[] parts = field
                                    .replace("[]", "")
                                    .replace("\"", "")
                                    .split("\\.");
                            
                            if (!d.isEmpty()) {
                                
                                Value currentValue = result.getValue(parts[0]);
                                
                                if (currentValue == null) {
                                    
                                    result.set(parts[0], d.getValue(parts[0]));
                                } else if (currentValue.getType() == Value.Type.ARRAY) {
                                    result.set(parts[0],
                                            Stream.concat(
                                                    currentValue.getList().stream(),
                                                    d.getList(parts[0]).stream()
                                            ).collect(Collectors.toList())
                                    );
                                } else if (currentValue.getType() == Value.Type.MAP) {
                                    result.set(parts[0], currentValue.getMap());
                                }
                            }
                            
                        } else if (document.getValue(field) != null) {
                            result.set(field, document.getValue(field));
                        }
                    });
            
            return result;
        }
    }
}
