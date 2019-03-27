package com.mapr.ojai.store.impl;

import com.mapr.db.impl.ConditionImpl;
import com.mapr.db.impl.MapRDBImpl;
import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryResult;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class InMemoryStore implements DocumentStore {
    
    
    private List<Document> documents = new ArrayList<>();
    
    
    @Override
    public boolean isReadOnly() {
        return false;
    }
    
    @Override
    public void flush() throws StoreException {
    
    }
    
    @Override
    public void beginTrackingWrites() throws StoreException {
    
    }
    
    @Override
    public void beginTrackingWrites(String previousWritesContext) throws StoreException {
    
    }
    
    @Override
    public String endTrackingWrites() throws StoreException {
        return null;
    }
    
    @Override
    public void clearTrackedWrites() throws StoreException {
    
    }
    
    @Override
    public Document findById(String _id) throws StoreException {
        return documents
                .stream()
                .filter(doc -> doc.getIdString().equals(_id))
                .findFirst()
                .orElse(null);
    }
    
    @Override
    public Document findById(Value _id) throws StoreException {
        return findById(_id.getString());
    }
    
    private Document project(Document document, String... fieldPaths) {
        Document result = MapRDBImpl.newDocument();
        
        Arrays.stream(fieldPaths)
                .forEach(field -> result.set(field, document.getValue(field)));
        
        return result;
    }
    
    @Override
    public Document findById(String _id, String... fieldPaths) throws StoreException {
        Document result = findById(_id);
        
        if (result == null) {
            return null;
        }
        
        return project(result, fieldPaths);
    }
    
    @Override
    public Document findById(String _id, FieldPath... fieldPaths) throws StoreException {
        return findById(_id, Arrays.stream(fieldPaths).map(FieldPath::getName).toArray(String[]::new));
    }
    
    @Override
    public Document findById(Value _id, String... fieldPaths) throws StoreException {
        return findById(_id.getString(), fieldPaths);
    }
    
    @Override
    public Document findById(Value _id, FieldPath... fieldPaths) throws StoreException {
        return findById(_id.getString(), fieldPaths);
    }
    
    @Override
    public Document findById(String _id, QueryCondition condition) throws StoreException {
        return null;
    }
    
    @Override
    public Document findById(Value _id, QueryCondition condition) throws StoreException {
        return null;
    }
    
    @Override
    public Document findById(String _id, QueryCondition condition, String... fieldPaths) throws StoreException {
        return null;
    }
    
    @Override
    public Document findById(String _id, QueryCondition condition, FieldPath... fieldPaths) throws StoreException {
        return null;
    }
    
    @Override
    public Document findById(Value _id, QueryCondition condition, String... fieldPaths) throws StoreException {
        return null;
    }
    
    @Override
    public Document findById(Value _id, QueryCondition condition, FieldPath... fieldPaths) throws StoreException {
        return null;
    }
    
    @Override
    public QueryResult find(Query query) throws StoreException {
        String jsonQuery = query.asJsonString();
        
        OjaiQuery ojaiQuery = (OjaiQuery) query;
        
        
        ConditionImpl condition = ojaiQuery.getCondition();
        
        
        Set<FieldPath> projections = ojaiQuery.getProjectedFieldSet();
        
        Document queryDoc = MapRDBImpl.newDocument(query.asJsonString());
        
        return null;
    }
    
    @Override
    public DocumentStream find() throws StoreException {
        return new DocumentStream() {
            @Override
            public void streamTo(DocumentListener documentListener) {
                documents.forEach(documentListener::documentArrived);
            }
            
            @Override
            public Iterator<Document> iterator() {
                return documents.iterator();
            }
            
            @Override
            public Iterable<DocumentReader> documentReaders() {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public void close() throws OjaiException {
            
            }
        };
    }
    
    @Override
    public DocumentStream findQuery(Query query) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream findQuery(String queryJSON) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream find(String... fieldPaths) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream find(FieldPath... fieldPaths) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream find(QueryCondition condition) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream find(QueryCondition condition, String... fieldPaths) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public DocumentStream find(QueryCondition condition, FieldPath... fieldPaths) throws StoreException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void insertOrReplace(Document doc) throws StoreException {
    
    }
    
    @Override
    public void insertOrReplace(String _id, Document r) throws StoreException {
    
    }
    
    @Override
    public void insertOrReplace(Value _id, Document doc) throws StoreException {
    
    }
    
    @Override
    public void insertOrReplace(Document doc, FieldPath fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void insertOrReplace(Document doc, String fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream) throws MultiOpException {
    
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void update(String _id, DocumentMutation mutation) throws StoreException {
    
    }
    
    @Override
    public void update(Value _id, DocumentMutation mutation) throws StoreException {
    
    }
    
    @Override
    public void delete(String _id) throws StoreException {
    
    }
    
    @Override
    public void delete(Value _id) throws StoreException {
    
    }
    
    @Override
    public void delete(Document doc) throws StoreException {
    
    }
    
    @Override
    public void delete(Document doc, FieldPath fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void delete(Document doc, String fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void delete(DocumentStream stream) throws MultiOpException {
    
    }
    
    @Override
    public void delete(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void delete(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void insert(String _id, Document doc) throws StoreException {
    
    }
    
    @Override
    public void insert(Value _id, Document doc) throws StoreException {
    
    }
    
    @Override
    public void insert(Document doc) throws StoreException {
    
    }
    
    @Override
    public void insert(Document doc, FieldPath fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void insert(Document doc, String fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void insert(DocumentStream stream) throws MultiOpException {
    
    }
    
    @Override
    public void insert(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void insert(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void replace(String _id, Document doc) throws StoreException {
    
    }
    
    @Override
    public void replace(Value _id, Document doc) throws StoreException {
    
    }
    
    @Override
    public void replace(Document doc) throws StoreException {
    
    }
    
    @Override
    public void replace(Document doc, FieldPath fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void replace(Document doc, String fieldAsKey) throws StoreException {
    
    }
    
    @Override
    public void replace(DocumentStream stream) throws MultiOpException {
    
    }
    
    @Override
    public void replace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void replace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
    
    }
    
    @Override
    public void increment(String _id, String field, byte inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, short inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, int inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, long inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, float inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, double inc) throws StoreException {
    
    }
    
    @Override
    public void increment(String _id, String field, BigDecimal inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, byte inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, short inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, int inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, long inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, float inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, double inc) throws StoreException {
    
    }
    
    @Override
    public void increment(Value _id, String field, BigDecimal inc) throws StoreException {
    
    }
    
    @Override
    public boolean checkAndMutate(String _id, QueryCondition condition, DocumentMutation mutation) throws StoreException {
        return false;
    }
    
    @Override
    public boolean checkAndMutate(Value _id, QueryCondition condition, DocumentMutation mutation) throws StoreException {
        return false;
    }
    
    @Override
    public boolean checkAndDelete(String _id, QueryCondition condition) throws StoreException {
        return false;
    }
    
    @Override
    public boolean checkAndDelete(Value _id, QueryCondition condition) throws StoreException {
        return false;
    }
    
    @Override
    public boolean checkAndReplace(String _id, QueryCondition condition, Document doc) throws StoreException {
        return false;
    }
    
    @Override
    public boolean checkAndReplace(Value _id, QueryCondition condition, Document doc) throws StoreException {
        return false;
    }
    
    @Override
    public void close() throws StoreException {
    
    }
}
