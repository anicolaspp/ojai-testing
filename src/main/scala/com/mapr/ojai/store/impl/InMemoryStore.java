package com.mapr.ojai.store.impl;

import com.mapr.db.impl.MapRDBImpl;
import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.MutationOp;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.QueryResult;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class InMemoryStore implements DocumentStore {
    
    private Connection connection;
    
    private ArrayList<Document> documents = new ArrayList<>();
    private String storeName;
    
    public InMemoryStore(String storeName, Connection connection) {
        
        this.storeName = storeName;
        this.connection = connection;
    }
    
    @Override
    public String toString() {
        return "InMemoryStore:" + storeName;
    }
    
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
        throw new UnsupportedOperationException();
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
        replace(doc);
    }
    
    @Override
    public void insertOrReplace(String _id, Document r) throws StoreException {
        replace(_id, r);
    }
    
    @Override
    public void insertOrReplace(Value _id, Document doc) throws StoreException {
        replace(_id, doc);
    }
    
    @Override
    public void insertOrReplace(Document doc, FieldPath fieldAsKey) throws StoreException {
        replace(doc, fieldAsKey);
    }
    
    @Override
    public void insertOrReplace(Document doc, String fieldAsKey) throws StoreException {
        replace(doc, fieldAsKey);
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream) throws MultiOpException {
        replace(stream);
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        replace(stream, fieldAsKey);
    }
    
    @Override
    public void insertOrReplace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        replace(stream, fieldAsKey);
    }
    
    
    private void getFromValue(String field, Value value, Document doc) {
        switch (value.getType()) {
            
            case BOOLEAN:
                value.getBoolean();
                break;
            case STRING:
                doc.set(field, value.getString());
                break;
            case BYTE:
                doc.set(field, value.getByte());
                break;
            case SHORT:
                doc.set(field, value.getShort());
                break;
            case INT:
                doc.set(field, value.getInt());
                break;
            case LONG:
                doc.set(field, value.getLong());
                break;
            case FLOAT:
                doc.set(field, value.getFloat());
                break;
            case DOUBLE:
                doc.set(field, value.getDouble());
                break;
            case DECIMAL:
                doc.set(field, value.getDecimal());
                break;
            case DATE:
                doc.set(field, value.getDate());
                break;
            case TIME:
                doc.set(field, value.getTime());
                break;
            case TIMESTAMP:
                doc.set(field, value.getTimestamp());
                break;
            case INTERVAL:
                doc.set(field, value.getInterval());
                break;
            case BINARY:
                doc.set(field, value.getBinary());
                break;
            case MAP:
                doc.set(field, value.getMap());
                break;
            case ARRAY:
                doc.set(field, value.getList());
                break;
            case NULL:
                doc.delete(field);
                break;
        }
        
        
    }
    
    @Override
    public void update(String _id, DocumentMutation mutation) throws StoreException {
        Document doc = findById(_id);
        
        if (doc == null) {
            doc = connection.newDocument().set("_id", _id);
        }
        
        delete(_id);
        
        InMemoryMutation mut = (InMemoryMutation) mutation;
        
        Iterator<MutationOp> it = mut.iterator();
        
        while (it.hasNext()) {
            MutationOp mutationOp = it.next();
    
            MutationOp.Type t = mutationOp.getType();
    
            Value.Type valueType = mutationOp.getOpValue().getType();
            getFromValue(mutationOp.getFieldPath().asPathString(), mutationOp.getOpValue(), doc);
        }
//
//        mut.iterator()
//                .forEachRemaining(mutationOp -> {
//
////
////                                switch (t) {
////
////                                    case SET:
////                                        getFromValue(mutationOp.getFieldPath().asPathString(), mutationOp.getOpValue(), entry);
////                                        break;
////                                    case SET_OR_REPLACE:
////                                        break;
////                                    case DELETE:
////                                        break;
////                                    case INCREMENT:
////                                        break;
////                                    case APPEND:
////                                        break;
////                                    case MERGE:
////                                        break;
////                                }
//
////                        entry.set(mutationOp.getFieldPath().asPathString(), mutationOp.getOpValue().getType())
//                        }
//                );
        
        
        insert(_id, doc);
    }
    
    @Override
    public void update(Value _id, DocumentMutation mutation) throws StoreException {
        update(_id.getString(), mutation);
    }
    
    private int index(String _id) {
        int idx = -1;
        
        for (Document doc : documents) {
            idx++;
            
            if (doc.getIdString().equals(_id)) {
                return idx;
            }
        }
        
        return -1;
    }
    
    @Override
    public void delete(String _id) throws StoreException {
        int idx = index(_id);
        
        if (idx >= 0) {
            documents.remove(idx);
        }
    }
    
    @Override
    public void delete(Value _id) throws StoreException {
        delete(_id.getString());
    }
    
    @Override
    public void delete(Document doc) throws StoreException {
        delete(doc.getId());
    }
    
    @Override
    public void delete(Document doc, FieldPath fieldAsKey) throws StoreException {
        delete(doc.getValue(fieldAsKey));
    }
    
    @Override
    public void delete(Document doc, String fieldAsKey) throws StoreException {
        delete(doc.getValue(fieldAsKey));
    }
    
    @Override
    public void delete(DocumentStream stream) throws MultiOpException {
        stream.forEach(this::delete);
    }
    
    @Override
    public void delete(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        stream.forEach(d -> delete(d, fieldAsKey));
    }
    
    @Override
    public void delete(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        stream.forEach(d -> delete(d, fieldAsKey));
    }
    
    @Override
    public void insert(String _id, Document doc) throws StoreException {
        checkId(_id);
        
        if (index(_id) < 0) {
            documents.add(doc);
        }
    }
    
    @Override
    public void insert(Value _id, Document doc) throws StoreException {
        checkId(_id);
        
        insert(_id.getString(), doc);
    }
    
    @Override
    public void insert(Document doc) throws StoreException {
        insert(doc.getId(), doc);
    }
    
    @Override
    public void insert(Document doc, FieldPath fieldAsKey) throws StoreException {
        insert(doc.getValue(fieldAsKey), doc);
    }
    
    @Override
    public void insert(Document doc, String fieldAsKey) throws StoreException {
        insert(doc.getValue(fieldAsKey), doc);
    }
    
    @Override
    public void insert(DocumentStream stream) throws MultiOpException {
        stream.forEach(this::insert);
    }
    
    @Override
    public void insert(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        stream.forEach(d -> insert(d.getValue(fieldAsKey), d));
    }
    
    @Override
    public void insert(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        stream.forEach(d -> insert(d.getValue(fieldAsKey), d));
    }
    
    @Override
    public void replace(String _id, Document doc) throws StoreException {
        delete(_id);
        
        documents.add(doc);
    }
    
    @Override
    public void replace(Value _id, Document doc) throws StoreException {
        replace(_id.getString(), doc);
    }
    
    @Override
    public void replace(Document doc) throws StoreException {
        replace(doc.getId(), doc);
    }
    
    @Override
    public void replace(Document doc, FieldPath fieldAsKey) throws StoreException {
        replace(doc.getValue(fieldAsKey), doc);
    }
    
    @Override
    public void replace(Document doc, String fieldAsKey) throws StoreException {
        replace(doc.getValue(fieldAsKey), doc);
    }
    
    @Override
    public void replace(DocumentStream stream) throws MultiOpException {
        stream.forEach(this::replace);
    }
    
    @Override
    public void replace(DocumentStream stream, FieldPath fieldAsKey) throws MultiOpException {
        stream.forEach(doc -> replace(doc.getValue(fieldAsKey), doc));
    }
    
    @Override
    public void replace(DocumentStream stream, String fieldAsKey) throws MultiOpException {
        stream.forEach(doc -> replace(doc.getValue(fieldAsKey), doc));
    }
    
    @Override
    public void increment(String _id, String field, byte inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.BYTE) {
                byte value = doc.getByte(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, short inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.SHORT) {
                short value = doc.getShort(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, int inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            
            if (doc.getValue(field).getType() == Value.Type.INT) {
                int value = doc.getInt(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, long inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.LONG) {
                long value = doc.getLong(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, float inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.FLOAT) {
                float value = doc.getFloat(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, double inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.DOUBLE) {
                double value = doc.getDouble(field);
                
                value += inc;
                
                doc.set(field, value);
            }
        }
    }
    
    @Override
    public void increment(String _id, String field, BigDecimal inc) throws StoreException {
        Document doc = findById(_id);
        
        if (doc != null) {
            if (doc.getValue(field).getType() == Value.Type.DECIMAL) {
                BigDecimal value = doc.getDecimal(field);
                
                doc.set(field, value.add(inc));
            }
        }
    }
    
    @Override
    public void increment(Value _id, String field, byte inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, short inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, int inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, long inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, float inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, double inc) throws StoreException {
        increment(_id.getString(), field, inc);
    }
    
    @Override
    public void increment(Value _id, String field, BigDecimal inc) throws StoreException {
        increment(_id.getString(), field, inc);
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
    
    private void checkId(Value _id) {
        if (_id == null) {
            throw new IllegalArgumentException("Missing _id");
        }
    }
    
    private void checkId(String _id) {
        if (_id == null || _id.isEmpty()) {
            throw new IllegalArgumentException("Missing _id");
        }
    }
}

