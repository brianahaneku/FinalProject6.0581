import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

class SerialFireWallWorker implements Runnable {
  PaddedPrimitiveNonVolatile<Boolean> done;
  final PacketGenerator source;
  final LBCAHashTable<AddressData> table;
  final AtomicIntegerArray histogram;
  long totalProcessedPackets = 0;

  public SerialFireWallWorker(PaddedPrimitiveNonVolatile<Boolean> done,
    PacketGenerator source,
    LBCAHashTable<AddressData> table) {
    this.done = done;
    this.source = source;
    this.table = table;
    this.histogram = new AtomicIntegerArray(new int[1 << 16]);
  }

  @Override
  public void run() {
    Packet pkt;
    AddressData sourceData;
    while (!done.value) {
      pkt = source.getPacket();
      switch (pkt.type) {
        case ConfigPacket:
        if (!table.contains(pkt.config.address)) {
          table.add(pkt.config.address, new AddressData());
        }
        sourceData = table.get(pkt.config.address);
        sourceData.updatePermission(pkt.config.personaNonGrata,
        pkt.config.acceptingRange,
         pkt.config.addressBegin,
          pkt.config.addressEnd);
          break;
      
        case DataPacket:
          if (!table.contains(pkt.header.source)) {
            table.add(pkt.header.source, new AddressData());
          }
          sourceData = table.get(pkt.header.source);
          if (!table.contains(pkt.header.dest)) {
            table.add(pkt.header.dest, new AddressData());
          }
          AddressData destData = table.get(pkt.header.dest);
          Boolean allowed = destData.cache.get(pkt.header.source);
          if (allowed == null) {
            allowed = destData.intervals.contains(pkt.header.source, sourceData);
            destData.cache.add(pkt.header.source, allowed);
          }
          if (allowed == true) {
            long fingerprint = Fingerprint.getFingerprint(pkt.body.iterations, pkt.body.seed);
            totalProcessedPackets++;
            histogram.getAndIncrement((int) fingerprint);
          }
          break;
      }
    }
  }
}

class Dispatcher implements Runnable {
  PaddedPrimitiveNonVolatile<Boolean> done;
  PacketGenerator source;
  final WaitFreeQueue<Packet>[] queues;
  Packet pkt;
  int index = 0;
  AtomicInteger packetsInFlight;

  public Dispatcher(PaddedPrimitiveNonVolatile<Boolean> done,
  PacketGenerator source,
  WaitFreeQueue<Packet>[] queues,
  AtomicInteger packetsInFlight) {
    this.done = done;
    this.source = source;
    this.queues = queues;
    this.pkt = null;
    this.packetsInFlight = packetsInFlight;
  }
  @Override
  public void run() {
    while ( !done.value ) {
      if (pkt == null) {
        pkt = source.getPacket();
      }
      if (packetsInFlight.get() < 256) {
        try {
          packetsInFlight.incrementAndGet();
          queues[index].enq(pkt);
          index = (index + 1) % queues.length;
          pkt = null;
        } catch (FullException e) {
          index = (index + 1) % queues.length;
        }
      }
    }
  }
}


class CacheMissWorker implements Runnable {
    PaddedPrimitiveNonVolatile<Boolean> done;
    final WaitFreeQueue<Packet> myQueue;
    final LBCAHashTable<AddressData> table;
    final AtomicIntegerArray histogram;
    AtomicInteger totalProcessedPackets;
    AtomicInteger packetsInFlight;

    public CacheMissWorker(
    PaddedPrimitiveNonVolatile<Boolean> done,
    WaitFreeQueue<Packet> myQueue,
    LBCAHashTable<AddressData> table,
    AtomicIntegerArray histogram,
    AtomicInteger totalProcessedPackets,
    AtomicInteger packetsInFlight) {
      this.done = done;
      this.table = table;
      this.myQueue = myQueue;
      this.histogram = histogram;
      this.totalProcessedPackets = totalProcessedPackets;
      this.packetsInFlight = packetsInFlight;
    }

    @Override
    public void run() {
        Packet pkt;
        while( !done.value ) {
        try {
            pkt = myQueue.deq();
            AddressData sourceData = table.get(pkt.header.source);
            AddressData destData = table.get(pkt.header.dest);
            LBCAHashTable<Boolean> cacheForDest = destData.cache;
            //Boolean allowed = (!sourceData.personaNonGrata) && (destData.intervals.contains(pkt.header.source));
            Boolean allowed = destData.intervals.contains(pkt.header.source, sourceData);
            cacheForDest.add(pkt.header.source, allowed);
            if (allowed) {
              long fingerprint = Fingerprint.getFingerprint(pkt.body.iterations, pkt.body.seed);
              this.histogram.incrementAndGet((int) fingerprint);
            }
            totalProcessedPackets.getAndIncrement();
        } catch (Throwable t) {
            //t.printStackTrace();
            continue;
            } finally {
              packetsInFlight.decrementAndGet();
            }
        }
    }  
}


class HeadWorker implements Runnable {
    PaddedPrimitiveNonVolatile<Boolean> done;
    final WaitFreeQueue<Packet> myQueue;
    final LBCAHashTable<AddressData> table;
    AtomicInteger totalProcessedPackets;
    final WaitFreeQueue<Packet>[] queues;
    AtomicIntegerArray histogram;
    AtomicInteger packetsInFlight;
    int index = 0;

    public HeadWorker(
    PaddedPrimitiveNonVolatile<Boolean> done,
    WaitFreeQueue<Packet> myQueue,
    LBCAHashTable<AddressData> table,
    AtomicInteger totalProcessedPackets,
    final WaitFreeQueue<Packet>[] queues,
    AtomicIntegerArray histogram,
    AtomicInteger packetsInFlight) {
      this.done = done;
      this.table = table;
      this.myQueue = myQueue;
      this.totalProcessedPackets = totalProcessedPackets;
      this.queues = queues;
      this.histogram = histogram;
      this.packetsInFlight = packetsInFlight;
    }

    @Override
  public void run() {
    Packet pkt;
    while( !done.value ) {
      try {
        pkt = myQueue.deq();
        AddressData sourceData;
        switch (pkt.type) {
          case ConfigPacket:
          if (!table.contains(pkt.config.address)) {
            table.add(pkt.config.address, new AddressData());
          }
          sourceData = table.get(pkt.config.address);
          sourceData.updatePermission(pkt.config.personaNonGrata,
          pkt.config.acceptingRange,
           pkt.config.addressBegin,
            pkt.config.addressEnd);
            totalProcessedPackets.getAndIncrement();
            packetsInFlight.decrementAndGet();
            break;
          
          case DataPacket:
            if (!table.contains(pkt.header.source)) {
              table.add(pkt.header.source, new AddressData());
            }
            sourceData = table.get(pkt.header.source);
            if (!table.contains(pkt.header.dest)) {
              table.add(pkt.header.dest, new AddressData());
            }
            AddressData destData = table.get(pkt.header.dest);
            Boolean allowed = destData.cache.get(pkt.header.source);
            if (allowed == null) {
              Boolean succeeded = false; // Have we found a queue to place the packet inside?
              while (!succeeded && !done.value) {
                try {
                  queues[index].enq(pkt);
                  index = (index + 1) % queues.length;
                  succeeded = true;
                } catch (FullException e) {
                  index = (index + 1) % queues.length;
                }
            }
          } else {
            if (allowed == true) {
              long fingerprint = Fingerprint.getFingerprint(pkt.body.iterations, pkt.body.seed);
              histogram.getAndIncrement((int) fingerprint);
            }
            totalProcessedPackets.getAndIncrement();
            packetsInFlight.decrementAndGet();
          }
        }
        
      } catch (Throwable e) {
        continue;
      }
    }
  } 
}
