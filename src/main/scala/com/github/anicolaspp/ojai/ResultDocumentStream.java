package com.github.anicolaspp.ojai;

import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.Connection;
import org.ojai.store.QueryResult;

import java.util.Iterator;
import java.util.stream.Stream;

public class ResultDocumentStream implements QueryResult {

    private Stream<Document> resultStream;
    private Connection connection;

    public ResultDocumentStream(Stream<Document> stream, Connection connection) {

        this.resultStream = stream;
        this.connection = connection;
    }

    @Override
    public Document getQueryPlan() {
        return connection.newDocument();
    }

    @Override
    public void streamTo(DocumentListener listener) {
        resultStream.forEach(listener::documentArrived);
    }

    @Override
    public Iterator<Document> iterator() {
        return resultStream.iterator();
    }

    @Override
    public Iterable<DocumentReader> documentReaders() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws OjaiException {

    }
}
