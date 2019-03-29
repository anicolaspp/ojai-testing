package com.github.anicolaspp.ojai;

import com.mapr.ojai.store.impl.InMemoryDriver;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

public interface JavaOjaiTesting extends OjaiTesting {
    
    @Override
    default public Connection connection() {
        DriverManager.registerDriver(InMemoryDriver.apply());
        
        return DriverManager.getConnection("ojai:anicolaspp:mem");
    }
    
    @Override
    default public DocumentStore documentStore(String storeName) {
        return connection().getStore(storeName);
    }
}
