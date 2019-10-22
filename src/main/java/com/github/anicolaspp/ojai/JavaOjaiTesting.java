package com.github.anicolaspp.ojai;

import com.mapr.ojai.store.impl.InMemoryDriver;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

public class JavaOjaiTesting implements OjaiTesting {

    private Connection connection;

    public JavaOjaiTesting() {
        DriverManager.registerDriver(InMemoryDriver.apply());

        connection = DriverManager.getConnection("ojai:anicolaspp:mem");
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public DocumentStore documentStore(String storeName) {
        return getConnection().getStore(storeName);
    }
}
