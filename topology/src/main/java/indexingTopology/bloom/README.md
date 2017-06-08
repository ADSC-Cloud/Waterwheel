3. How to use bloom filter?
    1. We can create bloom filters for different classes (including primitive data types wrapper classes and user-defined
    classes) through a BloomFilterHandler object.
        ```
        BloomFilterHandler bfHandler = new BloomFilterHandler("localFileName");
        BloomFilter<T> bf = bfHandler.createTBloomFilter();
        ```
       
    2. We can put any T object into this bloom filter.
        ```
        T t = new T();
        bfHandler.put(t);
        ```
        or
        ```
        T t = new T();
        bf.put(t);
        ```
    3. For persistent storage of bloom filters, we can store and load it to/from HDFS. Remember, when we reconstruct
    bloom filters from HDFS, we need to provide our class's funnel object so that the system knows how to deserialize
    the data.
        ```
        // write to HDFS
        bfHandler.store()
        
        // read from HDFS
        BloomFilter<T> bf = bfHandler.load(DataFunnel.getTFunnel); 
        ```
    4. We can get hint on whether an object is contained in the bloom filter.
        ```
        T t = new T();
        boolean b = bfHandler.mightContain(t);
        ```
        or
        ```
        T t = new T();
        boolean b = bf.mightContain(t);
        ```
    5. We can extend bloom filter to any classes.
    
          So far I have implemented bloom filters for all Java primitive types (boolean, char, short, int, long, float
          double) and String. If necessary, customized classes (such as Car) can be also implemented by simply implementing
          it Funnel by implementing Funnel<T> interface.