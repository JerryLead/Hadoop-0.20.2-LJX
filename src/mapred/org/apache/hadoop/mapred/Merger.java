/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

class Merger {  
  private static final Log LOG = LogFactory.getLog(Merger.class);

  // Local directories
  private static LocalDirAllocator lDirAlloc = 
    new LocalDirAllocator("mapred.local.dir");

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass, 
                            CompressionCodec codec,
                            Path[] inputs, boolean deleteInputs, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
  throws IOException {
    return 
      new MergeQueue<K, V>(conf, fs, inputs, deleteInputs, codec, comparator, 
                           reporter).merge(keyClass, valueClass,
                                           mergeFactor, tmpDir,
                                           readsCounter, writesCounter);
  }
  
  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                                   Class<K> keyClass, Class<V> valueClass,
                                   CompressionCodec codec,
                                   List<Segment<K, V>> segments, 
                                   int mergeFactor, Path tmpDir,
                                   RawComparator<K> comparator, Progressable reporter,
                                   Counters.Counter readsCounter,
                                   Counters.Counter writesCounter)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter, //MergeQueue是小根堆，sortSegments是false
        false, codec).merge(keyClass, valueClass,
            mergeFactor, tmpDir,
            readsCounter, writesCounter);

  }

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs, 
                            Class<K> keyClass, Class<V> valueClass, 
                            List<Segment<K, V>> segments, 
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return merge(conf, fs, keyClass, valueClass, segments, mergeFactor, tmpDir,
                 comparator, reporter, false, readsCounter, writesCounter);
  }

  public static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments).merge(keyClass, valueClass,
                                               mergeFactor, tmpDir,
                                               readsCounter, writesCounter);
  }

  static <K extends Object, V extends Object>
    RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                            Class<K> keyClass, Class<V> valueClass,
                            List<Segment<K, V>> segments,
                            int mergeFactor, int inMemSegments, Path tmpDir,
                            RawComparator<K> comparator, Progressable reporter,
                            boolean sortSegments,
                            Counters.Counter readsCounter,
                            Counters.Counter writesCounter)
      throws IOException {
    return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                           sortSegments).merge(keyClass, valueClass,
                                               mergeFactor, inMemSegments,
                                               tmpDir,
                                               readsCounter, writesCounter);
  }


  static <K extends Object, V extends Object>
  RawKeyValueIterator merge(Configuration conf, FileSystem fs,
                          Class<K> keyClass, Class<V> valueClass,
                          CompressionCodec codec,
                          List<Segment<K, V>> segments,
                          int mergeFactor, int inMemSegments, Path tmpDir,
                          RawComparator<K> comparator, Progressable reporter,
                          boolean sortSegments,
                          Counters.Counter readsCounter,
                          Counters.Counter writesCounter)
    throws IOException {
  return new MergeQueue<K, V>(conf, fs, segments, comparator, reporter,
                         sortSegments, codec).merge(keyClass, valueClass,
                                             mergeFactor, inMemSegments,
                                             tmpDir,
                                             readsCounter, writesCounter);
}

  public static <K extends Object, V extends Object>
  void writeFile(RawKeyValueIterator records, Writer<K, V> writer, 
                 Progressable progressable, Configuration conf) 
  throws IOException {
    long progressBar = conf.getLong("mapred.merge.recordsBeforeProgress",
        10000);
    long recordCtr = 0;
    while(records.next()) { //在records.next()中就调整堆，并且将下一个key和value设置好
      writer.append(records.getKey(), records.getValue());
      
      if (((recordCtr++) % progressBar) == 0) {
        progressable.progress();
      }
    }
}

  public static class Segment<K extends Object, V extends Object> {
    Reader<K, V> reader = null;
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;
    CompressionCodec codec = null;
    long segmentOffset = 0;
    long segmentLength = -1;
    
    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve) throws IOException {
      this(conf, fs, file, 0, fs.getFileStatus(file).getLen(), codec, preserve);
    }

    public Segment(Configuration conf, FileSystem fs, Path file,
        long segmentOffset, long segmentLength, CompressionCodec codec,
        boolean preserve) throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;

      this.segmentOffset = segmentOffset;
      this.segmentLength = segmentLength;
    }
    
    public Segment(Reader<K, V> reader, boolean preserve) {
      this.reader = reader;
      this.preserve = preserve;
      
      this.segmentLength = reader.getLength();
    }

    private void init(Counters.Counter readsCounter) throws IOException {
      if (reader == null) {
        FSDataInputStream in = fs.open(file); //打开/opt/hadooptmp/mapred/local/taskTracker/jobcache/job_201207061826_0003/attempt_201207061826_0003_m_000000_0/output/spill0.out
        in.seek(segmentOffset); //定位到开头
        reader = new Reader<K, V>(conf, in, segmentLength, codec, readsCounter); //构造reader
      }
    }
    
    DataInputBuffer getKey() { return key; }
    DataInputBuffer getValue() { return value; }

    long getLength() { 
      return (reader == null) ?
        segmentLength : reader.getLength();
    }
    
    boolean next() throws IOException {
      return reader.next(key, value);
    }
    
    void close() throws IOException {
      reader.close();
      
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }
  }
  
  private static class MergeQueue<K extends Object, V extends Object> 
  extends PriorityQueue<Segment<K, V>> implements RawKeyValueIterator {
    Configuration conf;
    FileSystem fs;
    CompressionCodec codec;
    
    List<Segment<K, V>> segments = new ArrayList<Segment<K,V>>();
    
    RawComparator<K> comparator;
    
    private long totalBytesProcessed;
    private float progPerByte;
    private Progress mergeProgress = new Progress();
    
    Progressable reporter;
    
    DataInputBuffer key;
    DataInputBuffer value;
    
    Segment<K, V> minSegment;
    Comparator<Segment<K, V>> segmentComparator =   
      new Comparator<Segment<K, V>>() {
      public int compare(Segment<K, V> o1, Segment<K, V> o2) {
        if (o1.getLength() == o2.getLength()) {
          return 0;
        }

        return o1.getLength() < o2.getLength() ? -1 : 1;
      }
    };

    
    public MergeQueue(Configuration conf, FileSystem fs, 
                      Path[] inputs, boolean deleteInputs, 
                      CompressionCodec codec, RawComparator<K> comparator,
                      Progressable reporter) 
    throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.codec = codec;
      this.comparator = comparator;
      this.reporter = reporter;
      
      for (Path file : inputs) {
        segments.add(new Segment<K, V>(conf, fs, file, codec, !deleteInputs));
      }
      
      // Sort segments on file-lengths
      Collections.sort(segments, segmentComparator); 
    }
    
    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter) {
      this(conf, fs, segments, comparator, reporter, false);
    }

    public MergeQueue(Configuration conf, FileSystem fs, 
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter, boolean sortSegments) {
      this.conf = conf;
      this.fs = fs;
      this.comparator = comparator;
      this.segments = segments;
      this.reporter = reporter;
      if (sortSegments) {
        Collections.sort(segments, segmentComparator);
      }
    }

    public MergeQueue(Configuration conf, FileSystem fs,
        List<Segment<K, V>> segments, RawComparator<K> comparator,
        Progressable reporter, boolean sortSegments, CompressionCodec codec) {
      this(conf, fs, segments, comparator, reporter, sortSegments);
      this.codec = codec;
    }

    public void close() throws IOException {
      Segment<K, V> segment;
      while((segment = pop()) != null) {
        segment.close();
      }
    }

    public DataInputBuffer getKey() throws IOException {
      return key;
    }

    public DataInputBuffer getValue() throws IOException {
      return value;
    }

    private void adjustPriorityQueue(Segment<K, V> reader) throws IOException{
      long startPos = reader.getPosition(); //4096
      boolean hasNext = reader.next();
      long endPos = reader.getPosition(); //4096
      totalBytesProcessed += endPos - startPos; 
      mergeProgress.set(totalBytesProcessed * progPerByte);
      if (hasNext) { //如果该Segment里面还有record，那么调整堆顶，否则将Segment移除堆
        adjustTop();
      } else {
        pop();
        reader.close();
      }
    }

    public boolean next() throws IOException {
      if (size() == 0)
        return false;

      if (minSegment != null) { //第一次调用时为空，以后都不会为null
        //minSegment is non-null for all invocations of next except the first
        //one. For the first invocation, the priority queue is ready for use
        //but for the subsequent invocations, first adjust the queue 
        adjustPriorityQueue(minSegment); //每写入一个record就调整一次优先级队列，使得包含key最小的Segment放到堆顶
        if (size() == 0) {
          minSegment = null;
          return false;
        }
      }
      minSegment = top(); //第一次最小的是/opt/hadooptmp/mapred/local/taskTracker/jobcache/job_201207061826_0003/attempt_201207061826_0003_m_000000_0/output/spill2.out
      
      key = minSegment.getKey(); //这里的key和value都是DataInputBuffer
      value = minSegment.getValue();

      return true;
    }

    @SuppressWarnings("unchecked")
    protected boolean lessThan(Object a, Object b) {
      DataInputBuffer key1 = ((Segment<K, V>)a).getKey(); //比较的时候只比较两个Segment当前指向的key
      DataInputBuffer key2 = ((Segment<K, V>)b).getKey();
      int s1 = key1.getPosition();
      int l1 = key1.getLength() - s1;
      int s2 = key2.getPosition();
      int l2 = key2.getLength() - s2;

      return comparator.compare(key1.getData(), s1, l1, key2.getData(), s2, l2) < 0;
    }
    
    public RawKeyValueIterator merge(Class<K> keyClass, Class<V> valueClass,
                                     int factor, Path tmpDir,
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter)
        throws IOException {
      return merge(keyClass, valueClass, factor, 0, tmpDir,
                   readsCounter, writesCounter);
    }

    RawKeyValueIterator merge(Class<K> keyClass, Class<V> valueClass,
                                     int factor, int inMem, Path tmpDir, //inMem = 0
                                     Counters.Counter readsCounter,
                                     Counters.Counter writesCounter)
        throws IOException {
      LOG.info("Merging " + segments.size() + " sorted segments");  //输出一次合并的segment数目
      
      //create the MergeStreams from the sorted map created in the constructor
      //and dump the final output to a file
      int numSegments = segments.size(); //spill{i}.out的个数，例如41
      int origFactor = factor; //默认是10
      int passNo = 1;
      do {
        //get the factor for this pass of merge. We assume in-memory segments
        //are the first entries in the segment list and that the pass factor
        //doesn't apply to them
        factor = getPassFactor(factor, passNo, numSegments - inMem); //10， 2， 37-0
        if (1 == passNo) {
          factor += inMem;
        } //factor = 5（第一轮）
        List<Segment<K, V>> segmentsToMerge =
          new ArrayList<Segment<K, V>>();
        int segmentsConsidered = 0;
        int numSegmentsToConsider = factor; //第一轮为5, 以后都为10
        long startBytes = 0; // starting bytes of segments of this merge
        while (true) {
          //extract the smallest 'factor' number of segments  
          //Call cleanup on the empty segments (no key/value data)
          List<Segment<K, V>> mStream = 
            getSegmentDescriptors(numSegmentsToConsider); //得到属于某个part的所有segment的
          for (Segment<K, V> segment : mStream) {
            // Initialize the segment at the last possible moment;
            // this helps in ensuring we don't use buffers until we need them
            segment.init(readsCounter); //spill0.out ~ spill4.out
            long startPos = segment.getPosition();
            boolean hasNext = segment.next();
            long endPos = segment.getPosition(); //4096,4096	2639
            startBytes += endPos - startPos; //4096,8152,...,20480
            
            if (hasNext) {
              segmentsToMerge.add(segment);
              segmentsConsidered++;
            }
            else {
              segment.close();
              numSegments--; //we ignore this segment for the merge
            }
          }
          //if we have the desired number of segments
          //or looked at all available segments, we break
          if (segmentsConsidered == factor || 
              segments.size() == 0) {
            break;
          }
            
          numSegmentsToConsider = factor - segmentsConsidered;
        }
        
        //feed the streams to the priority queue
        initialize(segmentsToMerge.size()); //new一个hadoop自己的PriorityQueue
        clear(); //清空priorityQueue
        for (Segment<K, V> segment : segmentsToMerge) {
          put(segment);
        }
        
        //if we have lesser number of segments remaining, then just return the
        //iterator, else do another single level merge
        if (numSegments <= factor) { //最后一次merge
          // Reset totalBytesProcessed to track the progress of the final merge.
          // This is considered the progress of the reducePhase, the 3rd phase
          // of reduce task. Currently totalBytesProcessed is not used in sort
          // phase of reduce task(i.e. when intermediate merges happen).
          totalBytesProcessed = startBytes;
          
          //calculate the length of the remaining segments. Required for 
          //calculating the merge progress
          long totalBytes = 0;
          for (int i = 0; i < segmentsToMerge.size(); i++) {
            totalBytes += segmentsToMerge.get(i).getLength();
          }
          if (totalBytes != 0) //being paranoid
            progPerByte = 1.0f / (float)totalBytes;
          
          if (totalBytes != 0)
            mergeProgress.set(totalBytesProcessed * progPerByte);
          else
            mergeProgress.set(1.0f); // Last pass and no segments left - we're done
          
          LOG.info("Down to the last merge-pass, with " + numSegments + 
                   " segments left of total size: " + totalBytes + " bytes");
          return this;
        } else {
        	/*
          LOG.info("Merging " + segmentsToMerge.size() + //Merging 5 intermediate， Merging 10 intermediate
                   " intermediate segments out of a total of " + 
                   (segments.size()+segmentsToMerge.size())); //41 + 5
          */
          //we want to spread the creation of temp files on multiple disks if 
          //available under the space constraints
          long approxOutputSize = 0; 
          for (Segment<K, V> s : segmentsToMerge) {
            approxOutputSize += s.getLength() + 
                                ChecksumFileSystem.getApproxChkSumLength(
                                s.getLength());  //1264585，估计segment的总大小 //2110916
          }                                     //2447720 //
          Path tmpFilename = //merge中间文件命名为attempt_201207061826_0003_m_000000_0/intermediate.1
            new Path(tmpDir, "intermediate").suffix("." + passNo);

          Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                              tmpFilename.toString(),
                                              approxOutputSize, conf);
          //added by LijieXu
          long currentRecord = writesCounter.getCounter();
          //added end
          Writer<K, V> writer = 
            new Writer<K, V>(conf, fs, outputFile, keyClass, valueClass, codec,
                             writesCounter);
      
          writeFile(this, writer, reporter, conf); //将多个segments合并输出到/opt/hadooptmp/mapred/local/attempt_201207061826_0003_m_000000_0/intermediate.1
          writer.close(); //注意是放在/opt/hadooptmp/mapred/local下面 attempt_201207061826_0003_m_000000_0/intermediate.2
          
          //added by LijieXu
          long writeRecords = writesCounter.getCounter() - currentRecord;
          long rawLength = writer.getRawLength();
          long compressedLength = writer.getCompressedLength();
          LOG.info("Merging " + segmentsToMerge.size() + //Merging 5 intermediate， Merging 10 intermediate
                  " intermediate segments out of a total of " + 
                  (segments.size()+segmentsToMerge.size()) 
                  + " <WriteRecords = " + writeRecords + ", RawLength = " + rawLength
                  + ", CompressedLength = " + compressedLength + ">"); //41 + 5
          //added end
          
          //we finished one single level merge; now clean up the priority 
          //queue
          this.close();

          // Add the newly create segment to the list of segments to be merged
          Segment<K, V> tempSegment = 
            new Segment<K, V>(conf, fs, outputFile, codec, false); //再将segment从intermediate.1中读出来
          segments.add(tempSegment); //41 - 5 + 1 = 37, 37 - 10 + 1 = 28, 28-10+1=19
          numSegments = segments.size();  //19-10+1=10
          Collections.sort(segments, segmentComparator); //将length比较短的排到前面
          
          passNo++;
        }
        //we are worried about only the first pass merge factor. So reset the 
        //factor to what it originally was
        factor = origFactor;
      } while(true);
    }
    
    /**
     * Determine the number of segments to merge in a given pass. Assuming more
     * than factor segments, the first pass should attempt to bring the total
     * number of segments - 1 to be divisible by the factor - 1 (each pass
     * takes X segments and produces 1) to minimize the number of merges.
     */
    private int getPassFactor(int factor, int passNo, int numSegments) { //e.g., 10, 1, 41
      if (passNo > 1 || numSegments <= factor || factor == 1) 
        return factor;
      int mod = (numSegments - 1) % (factor - 1); //(41 - 1) % (10 - 1) = 40 % 9 = 4
      if (mod == 0)
        return factor;
      return mod + 1; //5
    }
    
    /** Return (& remove) the requested number of segment descriptors from the
     * sorted map.
     */
    private List<Segment<K, V>> getSegmentDescriptors(int numDescriptors) {
      if (numDescriptors > segments.size()) {
        List<Segment<K, V>> subList = new ArrayList<Segment<K,V>>(segments);
        segments.clear();
        return subList;
      }
      
      List<Segment<K, V>> subList = 
        new ArrayList<Segment<K,V>>(segments.subList(0, numDescriptors));
      for (int i=0; i < numDescriptors; ++i) {
        segments.remove(0);
      }
      return subList;
    }

    public Progress getProgress() {
      return mergeProgress;
    }

  }
}
