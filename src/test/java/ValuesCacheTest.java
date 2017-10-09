import com.ndustrialio.cacheutils.*;
import com.ndustrialio.testy.Testy;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by jmhunt on 10/6/17.
 */
public class ValuesCacheTest
{
    Testy testy;

    Map<String, Object> env = new HashMap<>();

    ValuesCache timeScore, timeHashing;

    DateTime timestamp = new DateTime(2017, 10, 6, 13, 0, 0);

    @Before
    public void setup() throws Exception {

        testy = new Testy("testy.yaml");

        testy.init();

        env.put("redis_host", "localhost");

        timeHashing = new ValuesCache(new TimeHashingCache(new RedisClient(env, 1)));

        timeScore = new ValuesCache(new TimeScoreCache(new RedisClient(env, 2)));


        // Load test data
        timeHashing.put("testField1", 0, timestamp, "100");
        timeHashing.put("testField2", 0, timestamp, "200");
        timeHashing.put("testField3", 0, timestamp, "300");
        timeHashing.put("testField4", 0, timestamp, "400");


        timeScore.put("testField1", 0, timestamp, "100");
        timeScore.put("testField2", 0, timestamp, "200");
        timeScore.put("testField3", 0, timestamp, "300");
        timeScore.put("testField4", 0, timestamp, "400");

    }


    @Test
    public void runTest() throws Exception {

        // Test time hash single get
        Value<String> value = timeHashing.get("testField1", 0, timestamp);

        Assert.assertEquals(new Value<>(timestamp, "100"), value);

        // Test time score single get
        value = timeScore.get("testField1", 0, timestamp);

        Assert.assertEquals(new Value<>(timestamp, "100"), value);

        // Test time hash multi get
        List<Value<String>> values = timeHashing.get(timestamp, 0,
                "testField1", "testField2", "testField3", "testField4");

        Assert.assertEquals(Arrays.asList(new Value<>(timestamp, "100"),
                new Value<>(timestamp, "200"),
                new Value<>(timestamp, "300"),
                new Value<>(timestamp, "400")), values);

        // Test time hash multi get (keys misspelled on purpose)
        values = timeHashing.get(timestamp, 0,
                "testField1", "testField2", "testfield3", "testfield4");

        Assert.assertEquals(Arrays.asList(new Value<>(timestamp, "100"),
                new Value<>(timestamp, "200"),
                null,
                null), values);


        // Test time score multi get
        values = timeScore.get(timestamp, 0,
                "testField1", "testField2", "testField3", "testField4");

        Assert.assertEquals(Arrays.asList(new Value<>(timestamp, "100"),
                new Value<>(timestamp, "200"),
                new Value<>(timestamp, "300"),
                new Value<>(timestamp, "400")), values);

        // Test time score multi get (keys misspelled on purpose
        values = timeScore.get(timestamp, 0,
                "testField1", "testField2", "testfield3", "testfield4");

        Assert.assertEquals(Arrays.asList(new Value<>(timestamp, "100"),
                new Value<>(timestamp, "200"),
                null,
                null), values);


    }


    @After
    public void shutdown() throws Exception {
        testy.shutdown();
    }



}
