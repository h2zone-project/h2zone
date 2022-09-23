/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2020 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.*;
import java.time.Duration;
import java.time.Instant;


//import java.io.IOException;
import java.io.*;
import java.util.*;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.
 * <p>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>minfieldlength</b>: the minimum size of each field (default: 1)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just
 * one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record,
 * modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate
 * on - uniform, zipfian, hotspot, sequential, exponential or latest (default: uniform)
 * <LI><b>minscanlength</b>: for scans, what is the minimum number of records to scan (default: 1)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the
 * number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting record for this
 * YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of records for this
 * YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with string sort order by
 * 0 padding the record number. Controls the number of 0s to use for padding. (default: 1)
 * For example for row 5, with zeropadding=1 you get 'user5' key and with zeropading=8 you get
 * 'user00000005' key. In order to see its impact, zeropadding needs to be bigger than number of
 * digits in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed
 * order ("hashed") (default: hashed)
 * <LI><b>fieldnameprefix</b>: what should be a prefix for field names, the shorter may decrease the
 * required storage size (default: "field")
 * </ul>
 */


public class TraceReader {
  public static final String FILE_LIST = "SETTINGS_DIR/traces";
  public static final Integer LOAD_THRESHOLD = 1000000;

  private File file;
  private BufferedReader reader; 
  private int fileid;
  private Boolean stop;

  private ArrayList<String> filelist;

  private Long processed = 0L;
  private int bufferid;
  private ArrayList<String> bufferedkeys = new ArrayList<>();
  private ArrayList<String> bufferedoperations = new ArrayList<>();
  private ArrayList<Integer> bufferedvaluelengths = new ArrayList<>();

  public TraceReader() {
    filelist = new ArrayList<String>();

    File tfile = new File(this.FILE_LIST);
    BufferedReader treader = null;
    stop = false;

    try {
      treader = new BufferedReader(new FileReader(tfile));
      String text = null;

      while ((text = treader.readLine()) != null) {
        filelist.add(text);
        System.out.println("read file " + text);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (treader != null) {
          treader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (filelist.size() > 0) {
      openTraceFile(0);
      loadRequests();
    }
  }

  private void openTraceFile(int fid) {
    fileid = fid;

    if (fid >= filelist.size()) {
      System.out.println("Finished. " + processed + " requests in total");
      requestStop();
      return;
    }
    String fn = filelist.get(fileid);

    file = new File(fn);
    try {
      reader = new BufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } 
  }

  private void closeTraceFile() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void loadRequests() {
    List<String> fields;

    bufferid = 0;
    bufferedkeys.clear();
    bufferedoperations.clear();
    bufferedvaluelengths.clear();

    int times = LOAD_THRESHOLD;

    try {
      String text = null;
      Instant start = Instant.now();

      System.out.println("load from file " + filelist.get(fileid));

      // read at most 1M requests
      while (times > 0) {
        text = reader.readLine();
        if (text == null) {
          break;
        }
        fields = Arrays.asList(text.split(","));
        bufferedkeys.add(fields.get(1));
        bufferedoperations.add(fields.get(5));
        bufferedvaluelengths.add(Integer.parseInt(fields.get(3)));
        times--;
      }

      // Open a new file if the existing file finishes
      if (text == null && times == LOAD_THRESHOLD) {
        fileid++;
        closeTraceFile();
        openTraceFile(fileid);
      }

      Instant end = Instant.now();
      Duration timeElapsed = Duration.between(start, end);
      System.out.println("Time taken: " + timeElapsed.toMillis() +" milliseconds");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void nextRequest() {
    bufferid++;
    processed++;
    if (bufferid >= bufferedkeys.size()) {
      loadRequests();
      System.out.println("next " + bufferid + " " + processed);

      if (bufferid < bufferedkeys.size()) {
        System.out.println("     " + getKey() + " " + getValueLength() 
            + " " + getOperation());
      }
    }
  }

  public String getKey() {
    return bufferedkeys.get(bufferid);
  }

  public Integer getValueLength() {
    return bufferedvaluelengths.get(bufferid);
  }

  public String getOperation() {
    return bufferedoperations.get(bufferid);
  }

  private void requestStop() {
    stop = true;
  }

  public Boolean isStopRequested() {
    return stop;
  }
}
