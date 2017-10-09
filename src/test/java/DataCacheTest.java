import com.ndustrialio.cacheutils.DataCache;
import com.ndustrialio.cacheutils.Value;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jmhunt on 3/14/17.
 */
public class DataCacheTest
{
    @Test
    public void runTest() {

        DataCache<Integer> cache = new DataCache<>(10); // 10 second ttl

        DateTime baseTime = new DateTime(2017, 10, 9, 13, 0, 0);




        cache.put("multi1", baseTime, 1);
        cache.put("multi2", baseTime, 2);
        cache.put("multi3", baseTime, 3);
        cache.put("multi4", baseTime, 4);

        DateTime putTime = new DateTime(baseTime);

        for(int i = 0; i < 5; i++)
        {
            cache.put("test", putTime, i);

            putTime = putTime.plusMinutes(1);
        }


        DateTime targetTime = baseTime.plusMinutes(2).withSecondOfMinute(0).withMillisOfSecond(0);

        Value<Integer> v = cache.get("test", targetTime);

        Assert.assertEquals(v, new Value<>(targetTime, 2));

        v = cache.getClosestAfter("test", targetTime, true);

        Assert.assertEquals(v, new Value<>(targetTime, 2));

        // Try an exclusive get
        v = cache.getClosestAfter("test", targetTime, false);

        Assert.assertEquals(v, new Value<>(targetTime.plusMinutes(1), 3));

        v = cache.getClosestBefore("test", targetTime, true);

        Assert.assertEquals(v, new Value<>(targetTime, 2));

        // Try an exclusive get
        v = cache.getClosestBefore("test", targetTime, false);

        Assert.assertEquals(v, new Value<>(targetTime.minusMinutes(1), 1));

        cache.cleanup(10);


        // Test multi-get
        List<Value<Integer>> values = cache.get(baseTime, "multi1", "multi2", "multi3", "multi4");

        Assert.assertEquals(values, Arrays.asList(new Value<>(baseTime, 1), new Value<>(baseTime, 2),
                new Value<>(baseTime, 3), new Value<>(baseTime, 4)));

        // Throw in some nulls
        values = cache.get(baseTime, "multi1", "mulTi2", "multi3", "mUlti4");

        Assert.assertEquals(values, Arrays.asList(new Value<>(baseTime, 1), null,
                new Value<>(baseTime, 3), null));


    }
}
