/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.
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

package site.ycsb.generator;

import site.ycsb.Utils;
import java.util.ArrayList;
import java.io.*;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than
 * others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
 * of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
 * itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
 * you construct the instance, you can change the number of items by calling nextInt(itemcount) or nextLong(itemcount).
 * <p>
 * Unlike @ZipfianGenerator, this class scatters the "popular" items across the itemspace. Use this, instead of
 * @ZipfianGenerator, if you don't want the head of the distribution (the popular items) clustered together.
 */
public class ReadFromFileGenerator extends NumberGenerator {
  private final long min, max, itemcount;
  private final String filename = "SETTINGS_DIR/pattern.data";
  private ArrayList<Long> numbers = null;
  private int index;

  /******************************* Constructors **************************************/

  /**
   * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant. If you
   * use a zipfian constant other than 0.99, this will take a long time to complete because we need to recompute zeta.
   *
   * @param min             The smallest integer to generate in the sequence.
   * @param max             The largest integer to generate in the sequence.
   */
  public ReadFromFileGenerator(long min, long max) {
    this.min = min;
    this.max = max;
    itemcount = this.max - this.min + 1;
    System.out.println("max " + this.max + " min " + this.min + " itemcount "
        + itemcount + " this.itemcount " + this.itemcount);
    this.numbers = new ArrayList<Long>(); 
    index = 0;

    BufferedReader reader = null;

    int i = 0;

    File file = new File(this.filename);

    try {
      reader = new BufferedReader(new FileReader(file));
      String text = null;

      while ((text = reader.readLine()) != null) {
        Long num = Long.parseLong(text);
        if (i < 10) {
          System.out.println("ReadFromFileGenerator " + num + " read");
        }
        i++;
        this.numbers.add(num);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    System.out.println("ReadFromFileGenerator-total " + this.numbers.size());
  }

  /**************************************************************************************************/

  /**
   * Return the next long in the sequence.
   */
  @Override
  public Long nextValue() {
    long ret = this.numbers.get(index);
    if (index < 10) {
      System.out.println("(index) " + index + " - ret is " + ret + " hash is " + 
          Utils.fnvhash64(ret) + " itemcount is " + itemcount);
    }

    ret = min + Utils.fnvhash64(ret) % itemcount;
    setLastValue(ret);
    index++;
    if (index >= this.numbers.size()) {
      index = 0;
    }
    return ret;
  }

  public static void main(String[] args) {
    double newzetan = 1.0; // ZipfianGenerator.zetastatic(ITEM_COUNT, ZipfianGenerator.ZIPFIAN_CONSTANT);
    System.out.println("zetan: " + newzetan);
    System.exit(0);

    ReadFromFileGenerator gen = new ReadFromFileGenerator(0, 10000);

    for (int i = 0; i < 1000000; i++) {
      System.out.println("" + gen.nextValue());
    }
  }

  /**
   * since the values are scrambled (hopefully uniformly), the mean is simply the middle of the range.
   */
  @Override
  public double mean() {
    return ((min) + max) / 2.0;
  }
}
