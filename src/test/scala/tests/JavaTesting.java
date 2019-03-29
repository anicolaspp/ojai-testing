package tests;

import com.github.anicolaspp.ojai.InMemoryConnection;
import com.github.anicolaspp.ojai.JavaOjaiTesting;
import com.mapr.db.impl.InMemoryStore;
import org.junit.Test;
import org.scalatest.FlatSpec;

public class JavaTesting extends FlatSpec implements JavaOjaiTesting {
    
    @Test
    public void testGetConnection() {
        assert connection() instanceof InMemoryConnection;
    }
    
    @Test
    public void testGetStore() {
        assert documentStore("anicolaspp/java_store") instanceof InMemoryStore;
    }
}


