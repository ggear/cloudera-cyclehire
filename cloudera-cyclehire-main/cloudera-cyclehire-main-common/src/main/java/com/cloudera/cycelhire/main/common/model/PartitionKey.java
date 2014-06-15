package com.cloudera.cycelhire.main.common.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class PartitionKey implements WritableComparable<PartitionKey> {

  public static final String CODEC_NONE_LABEL = "deflate";

  public static final String PARTITION_UNKNOWN = "UNKNOWN";

  public static final Pattern REGEX_RECORD = Pattern.compile("([1-9][0-9]{9})_livecyclehireupdates\\.xml");
  public static final Pattern REGEX_BATCH = Pattern
      .compile("([1-9][0-9]{9})_([1-9][0-9]{9})_livecyclehireupdates\\.([a-z]+)\\.?([a-z]*)");

  private static final long EPOCH_MIN = 1280448000000L;
  private static final long EPOCH_MAX = System.currentTimeMillis() + 2 * 365 * 24 * 60 * 60 * 1000;

  private String batch = "";
  private String record = "";
  private long epochGet = 0L;
  private long epochUpdate = 0L;

  private byte version = 1;
  transient private Matcher recordMatcher = null;
  transient private Matcher batchMatcher = null;

  public PartitionKey() {
  }

  public PartitionKey batch(String batch) {
    this.batch = batch == null ? "" : batch;
    return this;
  }

  public PartitionKey record(String record) {
    this.record = record == null ? "" : record;
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

  public PartitionKey build() throws IllegalArgumentException {
    if (getBatchMatcher() == null) {
      throw new IllegalArgumentException("Illegal batch [" + batch + "]");
    }
    if (!record.equals("") && getRecordMatcher() == null) {
      throw new IllegalArgumentException("Illegal record [" + record + "]");
    }
    if (epochUpdate != 0L && (epochUpdate > EPOCH_MAX || epochUpdate < EPOCH_MIN)) {
      throw new IllegalArgumentException("Illegal update epoch [" + epochUpdate + "]");
    }
    if (epochGet != 0L
        && !record.equals("")
        && epochGet != 1000 * Long.parseLong(getRecordMatcher().group(1))
        || (epochGet = epochGet == 0L && !record.equals("") ? 1000 * Long.parseLong(getRecordMatcher().group(1))
            : epochGet) != 0L && (epochGet > EPOCH_MAX || epochGet < EPOCH_MIN)) {
      throw new IllegalArgumentException("Illegal get epoch [" + epochGet + "]");
    }
    if (epochGet != 0L
        && (epochGet < 1000 * Long.parseLong(getBatchMatcher().group(1)) || epochGet > 1000 * Long
            .parseLong(getBatchMatcher().group(2)))) {
      throw new IllegalArgumentException("Illegal get epoch [" + epochGet + "]");
    }
    return this;
  }

  protected boolean isValid() {
    try {
      build();
      return true;
    } catch (IllegalArgumentException exception) {
      return false;
    }
  }

  public String getType() throws IllegalArgumentException {
    return getBatchMatcher().group(3);
  }

  public String getCodec() throws IllegalArgumentException {
    return getBatchMatcher().group(4).equals("") ? CODEC_NONE_LABEL : getBatchMatcher().group(4);
  }

  public List<String> getPartitions() throws IllegalArgumentException {
    List<String> partitions = new ArrayList<String>();
    if (!record.equals("")) {
      Calendar calendar = getCalendarMonth(1000 * Long.parseLong(getRecordMatcher().group(1)));
      partitions.add("" + calendar.get(Calendar.YEAR) + "/" + (calendar.get(Calendar.MONTH) + 1));
    } else {
      Calendar calendarMin = getCalendarMonth(1000 * Long.parseLong(getBatchMatcher().group(1)));
      Calendar calendarMax = getCalendarMonth(1000 * Long.parseLong(getBatchMatcher().group(2)));
      while (calendarMin.before(calendarMax)) {
        partitions.add("" + calendarMin.get(Calendar.YEAR) + "/" + (calendarMin.get(Calendar.MONTH) + 1));
        calendarMin.add(Calendar.MONTH, 1);
      }
    }
    return partitions;
  }

  protected static Calendar getCalendarMonth(long epoch) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(epoch);
    DateUtils.truncate(calendar, Calendar.MONTH);
    return calendar;
  }

  private Matcher getRecordMatcher() throws IllegalArgumentException {
    if (recordMatcher == null) {
      if (!(recordMatcher = REGEX_RECORD.matcher(record)).matches()) {
        throw new IllegalArgumentException("Illegal record [" + record + "]");
      }
    }
    return recordMatcher;
  }

  private Matcher getBatchMatcher() throws IllegalArgumentException {
    if (batchMatcher == null) {
      if (!(batchMatcher = REGEX_BATCH.matcher(batch)).matches()) {
        throw new IllegalArgumentException("Illegal batch [" + batch + "]");
      }
    }
    return batchMatcher;
  }

  public String getRecord() {
    return record;
  }

  public String getBatch() {
    return batch;
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
    int compare = epochUpdate > that.epochUpdate ? +1 : epochUpdate < that.epochUpdate ? -1 : 0;
    compare = compare == 0 ? epochGet > that.epochGet ? +1 : epochGet < that.epochGet ? -1 : 0 : compare;
    compare = compare == 0 ? record.compareTo(that.record) : compare;
    return compare == 0 ? batch.compareTo(that.batch) : compare;
  }

}
