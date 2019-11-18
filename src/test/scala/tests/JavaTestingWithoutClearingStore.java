package tests;

import com.github.anicolaspp.ojai.JavaOjaiTesting;
import org.junit.Test;
import org.ojai.store.DocumentStore;

public class JavaTestingWithoutClearingStore extends JavaOjaiTesting {

    public JavaTestingWithoutClearingStore() {
        super(false);
    }

    @Test
    public void testStoreIsNotCleared() {
        DocumentStore store = documentStore("me");

        store.insert(getConnection().newDocument().setId("5"));
        store.close();

        DocumentStore sameStore = documentStore("me");
        assert sameStore.find().iterator().hasNext();
    }
}


