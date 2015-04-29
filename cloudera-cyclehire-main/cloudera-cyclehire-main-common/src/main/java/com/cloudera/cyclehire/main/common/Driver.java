package com.cloudera.cyclehire.main.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Driver extends Configured implements Tool {

  public static final int RETURN_SUCCESS = 0;
  public static final int RETURN_FAILURE_RUNTIME = 10;

  public static final String CONF_SETTINGS = "driver-site.xml";

  private static final int FORMAT_TIME_FACTOR = 10;

  private Map<String, Map<Enum<?>, Long>> counters = new LinkedHashMap<String, Map<Enum<?>, Long>>();

  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  public Driver() {
    super();
  }

  public Driver(Configuration conf) {
    super(conf);
  }

  public String description() {
    return "";
  }

  public String[] options() {
    return new String[0];
  }

  public String[] paramaters() {
    return new String[0];
  }

  public int prepare(String... arguments) throws Exception {
    return 0;
  }

  public abstract int execute() throws Exception;

  public int cleanup() throws Exception {
    return 0;
  }

  public void reset() {
    counters.clear();
  }

  @Override
  final public int run(String[] args) {

    if (LOG.isInfoEnabled()) {
      LOG.info("Driver [" + this.getClass().getSimpleName() + "] started");
    }

    long timeTotal = System.currentTimeMillis();

    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        try {
          cleanup();
        } catch (Exception exception) {
          if (LOG.isErrorEnabled()) {
            LOG.error("Exception raised executing shutdown handler", exception);
          }
        }
      }
    }, RunJar.SHUTDOWN_HOOK_PRIORITY + 1);

    if (Driver.class.getResource("/" + CONF_SETTINGS) != null) {
      getConf().addResource(CONF_SETTINGS);
    }

    reset();

    if (LOG.isDebugEnabled() && getConf() != null) {
      LOG.debug("Driver [" + this.getClass().getCanonicalName()
          + "] initialised with configuration properties:");
      for (Entry<String, String> entry : getConf())
        if (LOG.isDebugEnabled())
          LOG.debug("\t" + entry.getKey() + "=" + entry.getValue());
    }

    int exitValue = RETURN_FAILURE_RUNTIME;
    try {
      if ((exitValue = prepare(args)) == RETURN_SUCCESS) {
        exitValue = execute();
      }
    } catch (Exception exception) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Exception raised executing runtime pipeline handlers",
            exception);
      }
    } finally {
      try {
        cleanup();
      } catch (Exception exception) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Exception raised cleaning up runtime pipeline handlers",
              exception);
        }
      }
    }

    timeTotal = System.currentTimeMillis() - timeTotal;

    if (LOG.isInfoEnabled()) {
      LOG.info("Driver [" + this.getClass().getCanonicalName() + "] counters:");
      for (String group : getCountersGroups()) {
        Map<Enum<?>, Long> counters = getCounters(group);
        for (Enum<?> counter : counters.keySet()) {
          LOG.info("\t" + group + "." + counter.toString() + "="
              + counters.get(counter));
        }
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Driver [" + this.getClass().getSimpleName() + "] finshed "
          + (exitValue == RETURN_SUCCESS ? "successfully" : "unsuccessfully")
          + " with exit value [" + exitValue + "] in " + formatTime(timeTotal));
    }

    return exitValue;

  }

  final public int runner(String[] arguments) {
    int returnValue = 0;
    try {
      returnValue = ToolRunner.run(this, arguments);
    } catch (Exception exception) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Fatal error encountered", exception);
      }
      returnValue = RETURN_FAILURE_RUNTIME;
    }
    if (returnValue != RETURN_SUCCESS) {
      if (LOG.isInfoEnabled()) {
        StringBuilder optionsAndParamaters = new StringBuilder(256);
        for (int i = 0; i < options().length; i++) {
          optionsAndParamaters.append(" [-D" + options()[i] + "]");
        }
        for (int i = 0; i < paramaters().length; i++) {
          optionsAndParamaters.append(options().length == 0 ? "" : " " + "<"
              + paramaters()[i] + ">");
        }
        if (description() != null && !description().equals("")) {
          LOG.info("Description: " + description());
        }
        LOG.info("Usage: hadoop " + this.getClass().getCanonicalName()
            + " [generic options]" + optionsAndParamaters);
        ByteArrayOutputStream byteArrayPrintStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(byteArrayPrintStream);
        ToolRunner.printGenericCommandUsage(printStream);
        LOG.info(byteArrayPrintStream.toString());
        printStream.close();
      }
    }
    return returnValue;
  }

  public Map<String, Map<Enum<?>, Long>> getCounters() {
    return new LinkedHashMap<String, Map<Enum<?>, Long>>(counters);
  }

  public Map<Enum<?>, Long> getCounters(String group) {
    return counters.get(group) == null ? Collections.<Enum<?>, Long> emptyMap()
        : new LinkedHashMap<Enum<?>, Long>(counters.get(group));
  }

  public Set<String> getCountersGroups() {
    return new LinkedHashSet<String>(counters.keySet());
  }

  public Long getCounter(String group, Enum<?> counter) {
    return counters.get(group) == null
        || counters.get(group).get(counter) == null ? null : counters
        .get(group).get(counter);
  }

  protected void importCountersAll(Map<String, Map<Enum<?>, Long>> counters) {
    for (String group : counters.keySet()) {
      importCounters(group, counters.get(group));
    }
  }

  protected void importCounters(Map<Enum<?>, Long> counters) {
    importCounters(this.getClass().getCanonicalName(), counters);
  }

  protected void importCounters(String group, Map<Enum<?>, Long> counters) {
    if (this.counters.get(group) == null) {
      this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
    }
    for (Enum<?> value : counters.keySet()) {
      if (counters.get(value) != null) {
        this.counters.get(group).put(
            value,
            (this.counters.get(group).get(value) == null ? 0 : this.counters
                .get(group).get(value)) + counters.get(value));
      }
    }
  }

  protected void importCounters(Job job, Enum<?>[] values) throws IOException,
      InterruptedException {
    importCounters(this.getClass().getCanonicalName(), job, values);
  }

  protected void importCounters(String group, Job job, Enum<?>[] values)
      throws IOException, InterruptedException {
    if (this.counters.get(group) == null) {
      this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
    }
    Counters counters = job.getCounters();
    for (Enum<?> value : values) {
      if (counters.findCounter(value) != null) {
        this.counters.get(group).put(
            value,
            (this.counters.get(group).get(value) == null ? 0 : this.counters
                .get(group).get(value))
                + counters.findCounter(value).getValue());
      }
    }
  }

  public Long incrementCounter(Enum<?> counter, int incrament) {
    return incrementCounter(this.getClass().getCanonicalName(), counter,
        incrament);
  }

  public Long incrementCounter(String group, Enum<?> counter, int incrament) {
    if (this.counters.get(group) == null) {
      this.counters.put(group, new LinkedHashMap<Enum<?>, Long>());
    }
    return counters.get(group).put(
        counter,
        (counters.get(group).get(counter) == null ? 0 : counters.get(group)
            .get(counter)) + incrament);
  }

  public Long incrementCounter(Enum<?> counter, int incrament, String tag,
      Set<String> set) {
    return incrementCounter(this.getClass().getCanonicalName(), counter,
        incrament, tag, set);
  }

  public Long incrementCounter(String group, Enum<?> counter, int incrament,
      String tag, Set<String> set) {
    if (set.add(tag)) {
      return incrementCounter(group, counter, incrament);
    }
    return counters.get(group).get(counter);
  }

  private static String formatTime(long time) {
    StringBuilder string = new StringBuilder(128);
    int factor;
    String unit;
    if (time < 0) {
      time = 0;
      factor = 1;
      unit = "ms";
    } else if (time < FORMAT_TIME_FACTOR * 1000) {
      factor = 1;
      unit = "ms";
    } else if (time < FORMAT_TIME_FACTOR * 1000 * 60) {
      factor = 1000;
      unit = "sec";
    } else if (time < FORMAT_TIME_FACTOR * 1000 * 60 * 60) {
      factor = 1000 * 60;
      unit = "min";
    } else {
      factor = 1000 * 60 * 60;
      unit = "hour";
    }
    string.append("[");
    string.append(time / factor);
    string.append("] ");
    string.append(unit);
    return string.toString();
  }

}
