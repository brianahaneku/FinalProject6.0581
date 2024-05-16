import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;



class SerialFireWall {
    public static void main(String[] args) {
        int numAddressLog = Integer.parseInt(args[0]);
        int numTrainsLog = Integer.parseInt(args[1]);
        int meanTrainSize = Integer.parseInt(args[2]);
        int meanTrainsPerComm = Integer.parseInt(args[3]);
        int meanWindow = Integer.parseInt(args[4]);
        int meanCommsPerAddress = Integer.parseInt(args[5]);
        int meanWork = Integer.parseInt(args[6]);
        float configFraction = Float.parseFloat(args[7]);
        float pngFraction = Float.parseFloat(args[8]);
        float acceptingFraction = Float.parseFloat(args[9]);

        StopWatch timer = new StopWatch();
        PacketGenerator source = new PacketGenerator(numAddressLog,
         numTrainsLog,
          meanTrainSize,
           meanTrainsPerComm,
            meanWindow,
             meanCommsPerAddress,
              meanWork,
               configFraction,
                pngFraction,
                 acceptingFraction);
    
    PaddedPrimitiveNonVolatile<Boolean> done = new PaddedPrimitiveNonVolatile<Boolean>(false);
    LBCAHashTable<AddressData> addressTable = new LBCAHashTable<AddressData>(10, 5);
    for (int i = 0; i < Math.pow(1 << numAddressLog, 1.5); i++) {
        Packet pkt = source.getConfigPacket();
        if (!addressTable.contains(pkt.config.address)) {
            addressTable.add(pkt.config.address, new AddressData());
        }
        addressTable.get(pkt.config.address).updatePermission(pkt.config.personaNonGrata,
         pkt.config.acceptingRange,
          pkt.config.addressBegin, pkt.config.addressEnd);
    }
    SerialFireWallWorker workerData = new SerialFireWallWorker(done,
     source,
      addressTable);

    Thread workerThread = new Thread(workerData);
    workerThread.start();
    timer.startTimer();
    try {
        Thread.sleep(5000);
      } catch (InterruptedException ignore) {;}
    done.value = true;
    try {
    workerThread.join();
    } catch (InterruptedException ignore) {;}      
    timer.stopTimer();
    final long totalCount = workerData.totalProcessedPackets;
    System.out.println("count: " + totalCount);
    System.out.println("time: " + timer.getElapsedTime());
    System.out.println(totalCount/timer.getElapsedTime() + " pkts / ms");
}
}

class PipelineFirewall {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        int numAddressLog = Integer.parseInt(args[0]);
        int numTrainsLog = Integer.parseInt(args[1]);
        int meanTrainSize = Integer.parseInt(args[2]);
        int meanTrainsPerComm = Integer.parseInt(args[3]);
        int meanWindow = Integer.parseInt(args[4]);
        int meanCommsPerAddress = Integer.parseInt(args[5]);
        int meanWork = Integer.parseInt(args[6]);
        float configFraction = Float.parseFloat(args[7]);
        float pngFraction = Float.parseFloat(args[8]);
        float acceptingFraction = Float.parseFloat(args[9]);

        int numHeadWorkers = 8;
        int numCacheMissWorkers = 4;
        final int QUEUE_CAPACITY = 50;
        int numMilliseconds = 5000;

        StopWatch timer = new StopWatch();
        PacketGenerator source = new PacketGenerator(numAddressLog,
         numTrainsLog,
          meanTrainSize,
           meanTrainsPerComm,
            meanWindow,
             meanCommsPerAddress,
              meanWork,
               configFraction,
                pngFraction,
                 acceptingFraction);
    
    AtomicInteger totalProcessedPackets = new AtomicInteger(0);
    AtomicInteger packetsInFlight = new AtomicInteger();
    AtomicIntegerArray histogram = new AtomicIntegerArray(new int[1 << 16]);
    PaddedPrimitiveNonVolatile<Boolean> done = new PaddedPrimitiveNonVolatile<Boolean>(false);
    LBCAHashTable<AddressData> addressTable = new LBCAHashTable<AddressData>(10, 5);
    for (int i = 0; i < Math.pow(1 << numAddressLog, 1.5); i++) {
        Packet pkt = source.getConfigPacket();
        if (!addressTable.contains(pkt.config.address)) {
            addressTable.add(pkt.config.address, new AddressData());
        }
        addressTable.get(pkt.config.address).updatePermission(pkt.config.personaNonGrata,
         pkt.config.acceptingRange,
          pkt.config.addressBegin, pkt.config.addressEnd);
    }

    WaitFreeQueue<Packet>[] cacheMissWorkerQueues = new WaitFreeQueue[numCacheMissWorkers];
    for (int i = 0; i < numCacheMissWorkers; i++) {
        cacheMissWorkerQueues[i] = new WaitFreeQueue<Packet>(QUEUE_CAPACITY);
      }

      Thread[] cacheMissWorkerThreads = new Thread[numCacheMissWorkers];
      for (int i = 0; i < numCacheMissWorkers; i++) {
        CacheMissWorker cacheMissWorkerData = new CacheMissWorker(done,
        cacheMissWorkerQueues[i],
        addressTable,
        histogram,
        totalProcessedPackets,
        packetsInFlight);
        Thread cacheMissWorkerThread = new Thread(cacheMissWorkerData);
        cacheMissWorkerThreads[i] = cacheMissWorkerThread;
      }

    WaitFreeQueue<Packet>[] headWorkerQueues = new WaitFreeQueue[numHeadWorkers];
    for (int i = 0; i < numHeadWorkers; i++) {
        headWorkerQueues[i] = new WaitFreeQueue<Packet>(QUEUE_CAPACITY);
      }

    Thread[] headWorkerThreads = new Thread[numHeadWorkers];
    for (int i = 0; i < numHeadWorkers; i++) {
        HeadWorker headWorkerData = new HeadWorker(done,
         headWorkerQueues[i],
          addressTable,
           totalProcessedPackets,
            cacheMissWorkerQueues,
             histogram,
             packetsInFlight);
        Thread headWorkerThread = new Thread(headWorkerData);
        headWorkerThreads[i] = headWorkerThread;
    }

    Dispatcher dispatcherData = new Dispatcher(done, source, headWorkerQueues, packetsInFlight);
    Thread dispatcherThread = new Thread(dispatcherData);
    for (Thread thread : cacheMissWorkerThreads) {
        thread.start();
    }
    for (Thread thread : headWorkerThreads) {
        thread.start();
    }
    timer.startTimer();
    dispatcherThread.start();
    try {
        Thread.sleep(numMilliseconds);
    } catch (InterruptedException ignore) {;}
    done.value = true;
    try {
        dispatcherThread.join();
    } catch (InterruptedException ignore) {;}

    for (Thread thread: headWorkerThreads) {
        try {
            thread.join();
        } catch (InterruptedException ignore) {;}
      }
    for (Thread thread: cacheMissWorkerThreads) {
        try {
            thread.join();
        } catch (InterruptedException ignore) {;}
    }
    timer.stopTimer();
    final long totalCount = totalProcessedPackets.get();
    System.out.println("count: " + totalCount);
    System.out.println("time: " + timer.getElapsedTime());
    System.out.println(totalCount/timer.getElapsedTime() + " pkts / ms");
}
}