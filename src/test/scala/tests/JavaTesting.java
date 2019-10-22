package tests;

import com.github.anicolaspp.ojai.InMemoryConnection;
import com.github.anicolaspp.ojai.JavaOjaiTesting;
import com.mapr.db.impl.InMemoryStore;
import org.junit.Test;
import org.ojai.store.DocumentStore;

public class JavaTesting extends JavaOjaiTesting {

    @Test
    public void testGetConnection() {
        assert getConnection() instanceof InMemoryConnection;
    }

    @Test
    public void testGetStore() {
        assert documentStore("anicolaspp/java_store") instanceof InMemoryStore;
    }

    @Test
    public void testSameStore() {
        DocumentStore store = documentStore("me");

        store.insert(getConnection().newDocument().setId("5"));

        DocumentStore anotherStore = documentStore("me");

        assert anotherStore.find().iterator().hasNext();
    }
}


