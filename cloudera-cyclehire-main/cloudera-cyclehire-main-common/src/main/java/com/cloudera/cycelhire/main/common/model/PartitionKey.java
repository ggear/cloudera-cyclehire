package com.cloudera.cycelhire.main.common.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PartitionKey implements WritableComparable<PartitionKey> {

  public static final String CODEC_NONE_LABEL = "deflate";

  public static final Pattern REGEX_RECORD = Pattern
      .compile("([1-9][0-9]{9})_livecyclehireupdates\\.xml");
  public static final Pattern REGEX_BATCH = Pattern
      .compile("([1-9][0-9]{9})_([1-9][0-9]{9})_livecyclehireupdates\\.([a-z]+)\\.?([a-z]*)");
  public static final Pattern REGEX_PATH = Pattern
      .compile(".*\\/([a-z]+)\\/([a-z]+)\\/(year=[1-2][0-9]{3}\\/month=[1-9][0-9]?)\\/([1-9][0-9]{9}_[1-9][0-9]{9}_livecyclehireupdates\\.[a-z]+\\.?[a-z]*)\\/([1-9][0-9]{9}_?[1-9]?[0-9]{0,9}_livecyclehireupdates\\.[a-z]+\\.?[a-z]*).*");

  private static final long EPOCH_MIN = 1280448000000L;
  private static final long EPOCH_MAX = System.currentTimeMillis() + 100L
      * 365L * 24L * 60L * 60L * 1000L;

  private byte version = 1;

  private String batch = "";
  private String record = "";
  private long epochGet = 0L;
  private long epochUpdate = 0L;

  private String type = null;
  private String codec = null;
  private String partition = null;

  transient private Matcher recordMatcher = null;
  transient private Matcher batchMatcher = null;

  public static List<PartitionKey> getKeys(String batch, String record) {
    List<PartitionKey> keys = new ArrayList<PartitionKey>();
    for (String partition : new PartitionKey().batch(batch).record(record)
        .getPartitions()) {
      keys.add(new PartitionKey().batch(batch).record(record)
          .partition(partition));
    }
    return keys;
  }

  public PartitionKey() {
  }

  public PartitionKey batch(String batch) {
    this.batch = batch == null ? "" : batch;
    return this;
  }

  public PartitionKey record(String record) {
    this.record = record == null ? "" : record;
    if (getMatcherRecord() == null && REGEX_BATCH.matcher(record).matches()) {
      this.record = "";
    }
    if (this.epochGet == 0L && getMatcherRecord() != null) {
      epochGet(1000 * Long.parseLong(getMatcherRecord().group(1)));
    }
    return this;
  }

  public PartitionKey path(String path) {
    if (path != null) {
      Matcher matcher = REGEX_PATH.matcher(path);
      if (matcher.matches()) {
        batch(matcher.group(4));
        if (REGEX_RECORD.matcher(matcher.group(5)).matches()) {
          record(matcher.group(5));
        }
        partition(matcher.group(3));
      }
    }
    return this;
  }

  public PartitionKey partition(String partition) {
    this.partition = partition;
    return this;
  }

  public PartitionKey type(String type) {
    this.type = type;
    return this;
  }

  public PartitionKey codec(String codec) {
    this.codec = codec;
    return this;
  }

  public PartitionKey epochGet(long epochGet) {
    this.epochGet = epochGet;
    return this;
  }

  public PartitionKey epochUpdate(long epochUpdate) {
    this.epochUpdate = epochUpdate;
    return this;
  }

  public boolean isValid() {
    if (getMatcherBatch() == null) {
      return false;
    }
    long epochBatchMin = 1000 * Long.parseLong(getMatcherBatch().group(1));
    if (epochBatchMin > EPOCH_MAX || epochBatchMin < EPOCH_MIN) {
      return false;
    }
    long epochBatchMax = 1000 * Long.parseLong(getMatcherBatch().group(2));
    if (epochBatchMax > EPOCH_MAX || epochBatchMax < EPOCH_MIN) {
      return false;
    }
    if (epochBatchMax < epochBatchMin) {
      return false;
    }
    if (!record.equals("") && getMatcherRecord() == null) {
      return false;
    }
    long epochRecord = getMatcherRecord() == null ? 0L : 1000 * Long
        .parseLong(getMatcherRecord().group(1));
    if (epochRecord != 0L
        && (epochRecord > EPOCH_MAX || epochRecord < EPOCH_MIN)) {
      return false;
    }
    if (epochGet != 0L
        && (epochGet > EPOCH_MAX || epochGet < EPOCH_MIN || epochRecord != 0L
            && epochGet != epochRecord || epochGet > epochBatchMax || epochGet < epochBatchMin)) {
      return false;
    }
    if (epochUpdate != 0L
        && (epochUpdate > EPOCH_MAX || epochUpdate < EPOCH_MIN)) {
      return false;
    }
    if (!getPartitions().contains(getPartition())) {
      return false;
    }
    if (getType().equals(PartitionFlag._UNKNOWN.toString())) {
      return false;
    }
    if (getCodec().equals(PartitionFlag._UNKNOWN.toString())) {
      return false;
    }
    if (getPartition().equals(PartitionFlag._UNKNOWN.toString())) {
      return false;
    }
    if (getPartitions().equals(
        Arrays.asList(new String[] { PartitionFlag._UNKNOWN.toString() }))) {
      return false;
    }
    return true;
  }

  public String getType() {
    return type == null ? getMatcherBatch() == null ? PartitionFlag._UNKNOWN
        .toString() : getMatcherBatch().group(3) : type;
  }

  public String getCodec() {
    return codec == null ? getMatcherBatch() == null ? PartitionFlag._UNKNOWN
        .toString() : getMatcherBatch().group(4).equals("") ? CODEC_NONE_LABEL
        : getMatcherBatch().group(4) : codec;
  }

  public String getPartition() {
    Set<String> partitions = getPartitions();
    return partitions.size() == 1 ? partitions.iterator().next()
        : PartitionFlag._UNKNOWN.toString();
  }

  public Set<String> getPartitions() {
    Set<String> partitions = new TreeSet<String>();
    if (getMatcherBatch() != null) {
      Calendar calendarMin = null;
      Calendar calendarMax = null;
      Calendar calendarMinRange = getCalendarMonth(1000 * Long
          .parseLong(getMatcherBatch().group(1)));
      Calendar calendarMaxRange = getCalendarMonth(1000 * Long
          .parseLong(getMatcherBatch().group(2)));
      if (!record.equals("")) {
        if (getMatcherRecord() != null) {
          calendarMin = getCalendarMonth(1000 * Long
              .parseLong(getMatcherRecord().group(1)));
          calendarMax = getCalendarMonth(1000 * Long
              .parseLong(getMatcherRecord().group(1)));
        }
      } else {
        calendarMin = calendarMinRange;
        calendarMax = calendarMaxRange;
      }
      if (calendarMin != null
          && calendarMin.getTimeInMillis() < EPOCH_MAX
          && calendarMin.getTimeInMillis() > EPOCH_MIN
          && calendarMax != null
          && calendarMax.getTimeInMillis() < EPOCH_MAX
          && calendarMax.getTimeInMillis() > EPOCH_MIN
          && calendarMax.getTimeInMillis() >= calendarMin.getTimeInMillis()
          && calendarMin.getTimeInMillis() >= calendarMinRange
              .getTimeInMillis()
          && calendarMax.getTimeInMillis() <= calendarMaxRange
              .getTimeInMillis()) {
        while (calendarMin.getTimeInMillis() == calendarMax.getTimeInMillis()
            || calendarMin.before(calendarMax)) {
          partitions.add("year=" + calendarMin.get(Calendar.YEAR) + "/month="
              + (calendarMin.get(Calendar.MONTH) + 1));
          calendarMin.add(Calendar.MONTH, 1);
        }
      }
    }
    if (partition != null) {
      if (partitions.contains(partition)) {
        partitions.clear();
        partitions.add(partition);
      } else {
        partitions.clear();
      }
    }
    if (partitions.isEmpty()) {
      partitions.add(PartitionFlag._UNKNOWN.toString());
    }
    return partitions;
  }

  public String getPath() {
    return new StringBuilder(512).append('/').append(getType()).append('/')
        .append(getCodec()).append('/').append(getPartition()).append('/')
        .append(getBatch()).append('/').append(getRecord()).toString();
  }

  protected static Calendar getCalendarMonth(long epoch) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(epoch);
    DateUtils.truncate(calendar, Calendar.MONTH);
    return calendar;
  }

  private Matcher getMatcherRecord() {
    if (recordMatcher == null) {
      if (!(recordMatcher = REGEX_RECORD.matcher(record)).matches()) {
        recordMatcher = null;
      }
    }
    return recordMatcher;
  }

  private Matcher getMatcherBatch() {
    if (batchMatcher == null) {
      if (!(batchMatcher = REGEX_BATCH.matcher(batch)).matches()) {
        batchMatcher = null;
      }
    }
    return batchMatcher;
  }

  public String getRecord() {
    return record == null || record.equals("") ? getBatch() : record;
  }

  public String getBatch() {
    return batch == null || batch.equals("") ? PartitionFlag._UNKNOWN
        .toString() : batch;
  }

  public Long getEpochGet() {
    return epochGet;
  }

  public Long getEpochUpdate() {
    return epochUpdate;
  }

  @Override
  public String toString() {
    StringBuilder string = new StringBuilder();
    string.append("[batch=");
    string.append(batch);
    string.append(", record=");
    string.append(record);
    string.append(", get=");
    string.append(epochGet == 0 ? "" : new Date(epochGet));
    string.append(", update=");
    string.append(epochUpdate == 0 ? "" : new Date(epochUpdate));
    string.append("]");
    return string.toString();
  }

  @Override
  public int hashCode() {
    return 31 + (int) (epochUpdate ^ epochUpdate >>> 32);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object)
      return true;
    if (object == null)
      return false;
    if (getClass() != object.getClass())
      return false;
    return epochUpdate == ((PartitionKey) object).epochUpdate;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    version = in.readByte();
    record = WritableUtils.readString(in);
    batch = WritableUtils.readString(in);
    epochGet = WritableUtils.readVLong(in);
    epochUpdate = WritableUtils.readVLong(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(version);
    WritableUtils.writeString(out, record);
    WritableUtils.writeString(out, batch);
    WritableUtils.writeVLong(out, epochGet);
    WritableUtils.writeVLong(out, epochUpdate);
  }

  @Override
  public int compareTo(PartitionKey that) {
    int compare = epochUpdate > that.epochUpdate ? +1
        : epochUpdate < that.epochUpdate ? -1 : 0;
    compare = compare == 0 ? epochGet > that.epochGet ? +1
        : epochGet < that.epochGet ? -1 : 0 : compare;
    compare = compare == 0 ? record.compareTo(that.record) : compare;
    return compare == 0 ? batch.compareTo(that.batch) : compare;
  }

}
