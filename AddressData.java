import java.util.concurrent.atomic.AtomicBoolean;

public class AddressData {
    AtomicBoolean personaNonGrata;
    IntervalList intervals;
    LBCAHashTable<Boolean> cache;
    public static int HASH_TABLE_INITIAL_CAPACITY = 10;
    public static int HASH_TABLE_BUCKET_SIZE = 5;

    @SuppressWarnings("unchecked")
    public AddressData() {
        personaNonGrata = new AtomicBoolean();
        intervals = new IntervalList();
        cache = new LBCAHashTable<Boolean>(HASH_TABLE_INITIAL_CAPACITY, HASH_TABLE_BUCKET_SIZE);
    }

    public void updatePermission(boolean personaNonGrata,
     boolean acceptingRange, int addressBegin, int addressEnd) {
        clearCache();
        if (acceptingRange) {
            intervals.addInterval(addressBegin, addressEnd, this, personaNonGrata);
        } else {
            intervals.removeInterval(addressBegin, addressEnd, this, personaNonGrata);
        }
    }

    public void clearCache() {
        cache = new LBCAHashTable<Boolean>(HASH_TABLE_INITIAL_CAPACITY, HASH_TABLE_BUCKET_SIZE);
    }

    public void addToCache(int key, Boolean value) {
        cache.add(key, value);
        if (cache.capacity > 128) {
            clearCache();
        }
    }
}
