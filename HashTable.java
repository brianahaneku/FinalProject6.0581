import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ThreadLocalRandom;

public interface HashTable<T> {
	public void add(int key, T x);
	public boolean remove(int key);
	public boolean contains(int key);
}



class LBCAHashTable<T> implements HashTable<T> {
	/**
	 * Represents a lock based closed addressing hash table consisting of initially
	 * `capacity` locks which won't be resized while the table does in fact get resized according
	 * to a policy depending on `maxBucketSize`. Very closely follows the implementation from the textbook
	 * for the striped hash set in section 13.2.2.
	 */
	final ReadWriteLock[] readWriteLocks;
	private SerialList<T, Integer>[] table;
	private final int maxBucketSize;
	public int capacity;

	@SuppressWarnings("unchecked")
	public LBCAHashTable(int capacity, int maxBucketSize) {
		this.capacity = capacity;
		this.maxBucketSize = maxBucketSize;
		this.table = new SerialList[capacity];
		this.readWriteLocks = new ReadWriteLock[capacity];
		for (int i = 0; i < capacity; i++) {
			this.readWriteLocks[i] = new ReentrantReadWriteLock();
			this.table[i] = new SerialList<T,Integer>();
		}
	}

	/**
	 * Policy for resizing the table. If the bucket that key is in is larger than `maxBucketSize`, returns true.
	 */
	public boolean policy(Integer key) {
		acquireReadLock(maxBucketSize);
		try {
			return table[key.hashCode() % table.length] != null && table[key.hashCode() % table.length].getSize() > maxBucketSize;
		} finally {
			releaseReadLock(maxBucketSize);
		}
		
	}

	public void resizeIfNecessary(int key) {
		if (policy(key)) {
			resize();
		}
	}

	@SuppressWarnings("unchecked")
	public void resize() {
		int oldCapacity = table.length;
		acquireAllWriteLocks();
		try {
			if (oldCapacity != table.length) {
				return; //Some thread already resized the table
			}
			capacity = 2 * oldCapacity;
			SerialList<T,Integer>[] oldTable = table;
			table = new SerialList[capacity];
			for (int i = 0; i < capacity; i++) {
				table[i] = new SerialList<T, Integer>();
			}
			for (SerialList<T,Integer> bucket : oldTable) {
				SerialList<T,Integer>.Iterator<T,Integer> iterator = bucket.getHead(); //lmao
				while (iterator != null) {
					int index = iterator.key.hashCode() % table.length; // the index in the table that the key belongs
					table[index].add(iterator.key, iterator.getItem());
				iterator = iterator.getNext();
			}
		}
	} finally {
		releaseAllWriteLocks();
	}
}

	private void addNoCheck(int key, T x) {
		Integer y = key;
		int index = y.hashCode() % table.length;
		if(table[index] == null)
			table[index] = new SerialList<T,Integer>(key,x);
		else {
			table[index].add(key,x);
		}
	}

	public void add(int key, T x) {
		resizeIfNecessary(key);
		acquireWriteLock(key);
		try {
			addNoCheck(key, x);
		} finally {
			releaseWriteLock(key);
		}
	}

	public boolean remove(int key) {
		resizeIfNecessary(key);
		acquireWriteLock(key);
		Integer y = key;
		try {
			if (table[y.hashCode() % table.length] != null) {
				return table[y.hashCode() % table.length].remove(key);
			} 
			else {
				return false;
			}
		} finally {
			releaseWriteLock(key);
		}
	}

	public boolean contains(int key) {
		acquireReadLock(key);
		Integer y = key;
		try {
			if (table[y.hashCode() % table.length] != null) {
				return table[y.hashCode() % table.length].contains(key);
			} 
			else {
				return false;
			}
		} finally {
			releaseReadLock(key);
		}
	}

    public T get(int key) {
        acquireReadLock(key);
        
		try {
			Integer y = key;
            SerialList<T,Integer> bucket = table[y.hashCode() % table.length];
            SerialList<T,Integer>.Iterator<T,Integer> iterator = bucket.getItem(y);
            if (iterator != null) {
                return iterator.getItem();
            } else {
                return null;
            }
		} finally {
			releaseReadLock(key);
		}
    }


	public int hashFunction(Integer key) {
		return key.hashCode() % readWriteLocks.length;
	}

	public final void acquireWriteLock(int key) {
		readWriteLocks[hashFunction(key)].writeLock().lock();
	}

	public void releaseWriteLock(int key) {
		readWriteLocks[hashFunction(key)].writeLock().unlock();
	}

	public final void acquireReadLock(int key) {
		readWriteLocks[hashFunction(key)].readLock().lock();
	}

	public void releaseReadLock(int key) {
		readWriteLocks[hashFunction(key)].readLock().unlock();
	}

	public final void acquireAllWriteLocks() {
		for (ReadWriteLock lock: this.readWriteLocks) {
			lock.writeLock().lock();
		}
	}

	public void releaseAllWriteLocks() {
		for (ReadWriteLock lock: this.readWriteLocks) {
			lock.writeLock().unlock();
		}
	}

	public void printTable() {
		for( int i = 0; i < capacity; i++ ) {
			System.out.println("...." + i + "....");
			if( table[i] != null)
				table[i].printList();
		}
	}
}

class SerialHashTable<T> implements HashTable<T> {
	private SerialList<T,Integer>[] table;
	private int logSize;
	private int mask;
	private final int maxBucketSize;
	@SuppressWarnings("unchecked")
	public SerialHashTable(int logSize, int maxBucketSize) {
		this.logSize = logSize;
		this.mask = (1 << logSize) - 1;
		this.maxBucketSize = maxBucketSize;
		this.table = new SerialList[1 << logSize];
	}
	public void resizeIfNecessary(int key) {
		while( table[key & mask] != null 
					&& table[key & mask].getSize() >= maxBucketSize )
			resize();
	}
	private void addNoCheck(int key, T x) {
		int index = key & mask;
		if( table[index] == null )
			table[index] = new SerialList<T,Integer>(key,x);
		else
			table[index].addNoCheck(key,x);
	}
	public void add(int key, T x) {
		resizeIfNecessary(key);
		addNoCheck(key,x);
	}
	public boolean remove(int key) {
		resizeIfNecessary(key);
		if( table[key & mask] != null )
			return table[key & mask].remove(key);
		else
			return false;
	}
	public boolean contains(int key) {
		SerialList<T,Integer>[] myTable = table;
		int myMask = myTable.length - 1;
		if( myTable[key & myMask] != null )
			return myTable[key & myMask].contains(key);
		else
			return false;
	}
	@SuppressWarnings("unchecked")
	public void resize() {
		SerialList<T,Integer>[] newTable = new SerialList[2*table.length];
		for( int i = 0; i < table.length; i++ ) {
			if( table[i] == null )
				continue;
			SerialList<T,Integer>.Iterator<T,Integer> iterator = table[i].getHead();
			while( iterator != null ) {
				if( newTable[iterator.key & ((2*mask)+1)] == null )
					newTable[iterator.key & ((2*mask)+1)] = new SerialList<T,Integer>(iterator.key, iterator.getItem());
				else
					newTable[iterator.key & ((2*mask)+1)].addNoCheck(iterator.key, iterator.getItem());
				iterator = iterator.getNext();
			}
		}
		table = newTable;
		logSize++;
		mask = (1 << logSize) - 1;
	}
	public void printTable() {
		for( int i = 0; i <= mask; i++ ) {
			System.out.println("...." + i + "....");
			if( table[i] != null)
				table[i].printList();
		}
	}
}

class SerialHashTableTest {
	public static void main(String[] args) {  
		SerialHashTable<Integer> table = new SerialHashTable<Integer>(2, 8);
		for( int i = 0; i < 256; i++ ) {
			table.add(i,i*i);
		}
		table.printTable();    
	}
}

class LBCAHashTableTest {
	public static void main(String[] args) {
		LBCAHashTable<Integer> table = new LBCAHashTable<Integer>(4, 8);
		for( int i = 0; i < 256; i++ ) {
			table.add(i,i*i);
		}
		table.printTable(); 
		System.out.println("hello world"); 
	}
}


