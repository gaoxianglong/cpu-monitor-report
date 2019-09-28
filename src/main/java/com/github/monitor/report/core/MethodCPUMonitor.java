package com.github.monitor.report.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.jvm.sandbox.api.Information;
import com.alibaba.jvm.sandbox.api.Module;
import com.alibaba.jvm.sandbox.api.ModuleLifecycle;
import com.alibaba.jvm.sandbox.api.http.Http;
import com.alibaba.jvm.sandbox.api.listener.ext.Advice;
import com.alibaba.jvm.sandbox.api.listener.ext.AdviceListener;
import com.alibaba.jvm.sandbox.api.listener.ext.EventWatchBuilder;
import com.alibaba.jvm.sandbox.api.listener.ext.EventWatcher;
import com.alibaba.jvm.sandbox.api.resource.ModuleEventWatcher;
import com.taobao.diamond.manager.ManagerListener;
import com.yunji.diamond.client.api.DiamondClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 监控目标方法的CPU使用率,I/O操作使用率
 *
 * @author gaoxianglong
 */
@MetaInfServices(Module.class)
@Information(id = "methodCpuMonitor", isActiveOnLoad = true, author = "gxl", version = "0.0.1")
public class MethodCPUMonitor implements Module, ModuleLifecycle {
    private Logger log = LoggerFactory.getLogger(MethodCPUMonitor.class);
    @Resource
    private ModuleEventWatcher moduleEventWatcher;
    private OperatingSystemMXBean osMxBean;
    private ThreadMXBean threadBean;
    private String applicationName;
    private Map<String, AtomicReference<Object[]>> map;
    private KafkaProducer kafkaProducer;
    private DiamondClient diamondClient;
    private String kafkaHost;
    private boolean isReport = true;
    private ScheduledExecutorService service;
    /**
     * 上报时间周期缺省30s一次
     */
    private int reportTime = 30;
    private EventWatcher eventWatcher;
    private static final String DATA_ID = "methodCpuMonitor";
    private static final String TOPIC = "methodCpuMonitor-topic";
    private String[] blackList;

    @Override
    public void onLoad() throws Throwable {
        log.info("onLoad MethodCPUMonitor model");
    }

    @Override
    public void onUnload() throws Throwable {
        eventWatcher.onUnWatched();
        eventWatcher = null;
        moduleEventWatcher = null;
        osMxBean = null;
        threadBean = null;
        applicationName = null;
        map = null;
        kafkaProducer.close();
        service.shutdown();
        log.info("onUnload MethodCPUMonitor model");
    }

    @Override
    public void onActive() throws Throwable {
        log.info("onActive MethodCPUMonitor model");
    }

    @Override
    public void onFrozen() throws Throwable {
        log.info("onFrozen MethodCPUMonitor model");
    }

    /**
     * 触发监控模块
     *
     * @author gaoxianglong
     */
    @Http("/monitor")
    public void monitor() {
        if (null != eventWatcher) {
            return;
        }
        log.info("execute MethodCPUMonitor model");
        eventWatcher = new EventWatchBuilder(moduleEventWatcher).onClass("com.yunji.*")
                .onBehavior("*").onWatch(new AdviceListener() {


                    @Override
                    protected void afterReturning(Advice advice) throws Throwable {
                        TimeBO tb = advice.attachment();
                        tb.setEndCurrentThreadCpuTime(threadBean.getCurrentThreadCpuTime());
                        tb.setEndTime(System.nanoTime());
                        count(advice);
                    }

                    @Override
                    protected void afterThrowing(Advice advice) throws Throwable {
                        afterReturning(advice);
                    }

                    @Override
                    protected void before(Advice advice) throws Throwable {
                        TimeBO tb = new TimeBO();
                        tb.setBeginTime(System.nanoTime());
                        tb.setBeginCurrentThreadCpuTime(threadBean.getCurrentThreadCpuTime());
                        advice.attach(tb);
                    }

                    void count(Advice advice) throws Throwable {
                        String targetClassName = advice.getBehavior().getDeclaringClass().getName();
                        if (!isReport) {
                            return;
                        }
                        if (null != blackList) {
                            for (String list : blackList) {
                                if (targetClassName.startsWith(list)) {
                                    return;
                                }
                            }
                        }
                        TimeBO tb = advice.attachment();
                        long runTimeResult = tb.getEndTime() - tb.getBeginTime(); /* 程序的总执行时间 */
                        long cpuRunTimeResult = tb.getEndCurrentThreadCpuTime() - tb.getBeginCurrentThreadCpuTime(); /* CPU的总执行时间 */
                        String targetMethodName = advice.getBehavior().getName();
                        String key = String.format("%s.%s", targetClassName, targetMethodName);
                        AtomicReference<Object[]> reference = map.get(key);
                        if (null == reference) {
                            reference = new AtomicReference<>();
                            map.put(key, reference);
                        }
                        /* 如果程序的总执行时间低于10ms,则不采集,因为总执行时间短且CPU总执行时间还接近于程序总执行时间的话,这样的采集通常是毫无意义的 */
                        if ((runTimeResult / 0xf4240) >= 10) {
                            Object[] source = null;
                            Object[] target = new Object[12];
                            do {
                                source = reference.get();
                                if (null == source) {
                                    target[0] = applicationName;
                                    target[1] = HostAddressUtil.getLocalAddress();
                                    target[2] = reportTime;
                                    target[3] = targetClassName;
                                    target[4] = targetMethodName;
                                    target[5] = 0;
                                    target[6] = 0;
                                    target[7] = 0.0d;
                                    target[8] = 0.0d;
                                    target[9] = 0l;
                                    target[10] = 0l;
                                    target[11] = 0l;
                                } else {
                                    target[0] = source[0];
                                    target[1] = source[1];
                                    target[2] = source[2];
                                    target[3] = source[3];
                                    target[4] = source[4];
                                    if (advice.isThrows() || null != advice.getThrowable()) {
                                        target[5] = (int) source[5] + 1;
                                        target[6] = source[6];
                                    } else {
                                        target[5] = source[5];
                                        target[6] = (int) source[6] + 1;
                                    }
                                    target[7] = (double) source[7] + ((double) cpuRunTimeResult / runTimeResult * 100);/* 目标方法的CPU使用率 */
                                    target[8] = (double) source[8] + ((double) (runTimeResult - cpuRunTimeResult) / runTimeResult * 100);/* 目标方法的IO使用率 */
                                    target[9] = (long) source[9] + cpuRunTimeResult / 0xf4240;/* CPU执行时间 */
                                    target[10] = (long) source[10] + ((runTimeResult - cpuRunTimeResult) / 0xf4240);/* IO耗时，单位ms */
                                    target[11] = (long) source[11] + runTimeResult / 0xf4240;/* 程序执行时间 */
                                }
                            } while (!reference.compareAndSet(source, target));
                        }
                    }

                    class TimeBO {
                        long beginTime;
                        long endTime;
                        long beginCurrentThreadCpuTime;
                        long endCurrentThreadCpuTime;

                        @Override
                        public String toString() {
                            return "TimeBO{" +
                                    "beginTime=" + beginTime +
                                    ", endTime=" + endTime +
                                    ", beginCurrentThreadCpuTime=" + beginCurrentThreadCpuTime +
                                    ", endCurrentThreadCpuTime=" + endCurrentThreadCpuTime +
                                    '}';
                        }

                        public long getBeginCurrentThreadCpuTime() {
                            return beginCurrentThreadCpuTime;
                        }

                        public void setBeginCurrentThreadCpuTime(long beginCurrentThreadCpuTime) {
                            this.beginCurrentThreadCpuTime = beginCurrentThreadCpuTime;
                        }

                        public long getEndCurrentThreadCpuTime() {
                            return endCurrentThreadCpuTime;
                        }

                        public void setEndCurrentThreadCpuTime(long endCurrentThreadCpuTime) {
                            this.endCurrentThreadCpuTime = endCurrentThreadCpuTime;
                        }

                        public long getBeginTime() {
                            return beginTime;
                        }

                        public void setBeginTime(long beginTime) {
                            this.beginTime = beginTime;
                        }

                        public long getEndTime() {
                            return endTime;
                        }

                        public void setEndTime(long endTime) {
                            this.endTime = endTime;
                        }
                    }
                });
    }

    class ResultBO {
        /**
         * 程序的总执行时间
         */
        long runTimeResult;
        /**
         * CPU的总执行时间
         */
        long cpuRunTimeResult;
        /**
         * 目标类全限定名
         */
        String targetClassName;
        /**
         * 目标方法
         */
        String targetMethodName;
        /**
         * 目标方法的CPU使用率
         */
        double cpuUsage;
        /**
         * 目标方法的IO使用率
         */
        double ioUsage;
        /**
         * IO总耗时，单位ms
         */
        long ioTimeResult;
        /**
         * 目标方法的成功执行次数
         */
        int successNum;
        /**
         * 目标方法的失败执行次数
         */
        int failNum;
        /**
         * 应用名称
         */
        String applicationName;
        String host;
        /**
         * 上报周期
         */
        int reportTime;

        /**
         * 当前时间戳
         */
        long createTime;

        @Override
        public String toString() {
            return "ResultBO{" +
                    "runTimeResult=" + runTimeResult +
                    ", cpuRunTimeResult=" + cpuRunTimeResult +
                    ", targetClassName='" + targetClassName + '\'' +
                    ", targetMethodName='" + targetMethodName + '\'' +
                    ", cpuUsage=" + cpuUsage +
                    ", ioUsage=" + ioUsage +
                    ", ioTimeResult=" + ioTimeResult +
                    ", successNum=" + successNum +
                    ", failNum=" + failNum +
                    ", applicationName='" + applicationName + '\'' +
                    ", host='" + host + '\'' +
                    ", reportTime=" + reportTime +
                    ", createTime=" + createTime +
                    '}';
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getReportTime() {
            return reportTime;
        }

        public void setReportTime(int reportTime) {
            this.reportTime = reportTime;
        }

        public String getApplicationName() {
            return applicationName;
        }

        public void setApplicationName(String applicationName) {
            this.applicationName = applicationName;
        }

        public long getRunTimeResult() {
            return runTimeResult;
        }

        public void setRunTimeResult(long runTimeResult) {
            this.runTimeResult = runTimeResult;
        }

        public long getCpuRunTimeResult() {
            return cpuRunTimeResult;
        }

        public void setCpuRunTimeResult(long cpuRunTimeResult) {
            this.cpuRunTimeResult = cpuRunTimeResult;
        }

        public String getTargetClassName() {
            return targetClassName;
        }

        public void setTargetClassName(String targetClassName) {
            this.targetClassName = targetClassName;
        }

        public String getTargetMethodName() {
            return targetMethodName;
        }

        public void setTargetMethodName(String targetMethodName) {
            this.targetMethodName = targetMethodName;
        }

        public double getCpuUsage() {
            return cpuUsage;
        }

        public void setCpuUsage(double cpuUsage) {
            this.cpuUsage = cpuUsage;
        }

        public double getIoUsage() {
            return ioUsage;
        }

        public void setIoUsage(double ioUsage) {
            this.ioUsage = ioUsage;
        }

        public long getIoTimeResult() {
            return ioTimeResult;
        }

        public void setIoTimeResult(long ioTimeResult) {
            this.ioTimeResult = ioTimeResult;
        }

        public int getSuccessNum() {
            return successNum;
        }

        public void setSuccessNum(int successNum) {
            this.successNum = successNum;
        }

        public int getFailNum() {
            return failNum;
        }

        public void setFailNum(int failNum) {
            this.failNum = failNum;
        }
    }

    @Override
    public void loadCompleted() {
        osMxBean = ManagementFactory.getOperatingSystemMXBean();
        threadBean = ManagementFactory.getThreadMXBean();
        applicationName = System.getProperty("applicationName");
        map = new ConcurrentHashMap<>(32);
        service = Executors
                .newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!isReport || null == map) {
                        return;
                    }
                    for (Map.Entry<String, AtomicReference<Object[]>> entry : map.entrySet()) {
                        String key = entry.getKey();
                        AtomicReference<Object[]> reference = entry.getValue();
                        Object[] source = reference.get();
                        if (null == source) {
                            continue;
                        }
                        String applicationName = (String) source[0];
                        String host = (String) source[1];
                        int reportTime = (int) source[2];
                        String targetClassName = (String) source[3];
                        String targetMethodName = (String) source[4];
                        int failNum = (int) source[5];
                        int successNum = (int) source[6];
                        double cpuUsage = (double) source[7];
                        double ioUsage = (double) source[8];
                        long cpuRunTimeResult = (long) source[9];
                        long ioTimeResult = (long) source[10];
                        long runTimeResult = (long) source[11];
                        int num = successNum + failNum;/* 单位时间内的总执行次数 */
                        ResultBO rb = new ResultBO();
                        rb.setApplicationName(applicationName);
                        rb.setTargetClassName(targetClassName);
                        rb.setTargetMethodName(targetMethodName);
                        rb.setHost(host);
                        rb.setReportTime(reportTime);
                        rb.setFailNum(failNum);
                        rb.setSuccessNum(successNum);
                        if (num > 0) {
                            rb.setCpuUsage(Double.parseDouble(String.format("%.1f", cpuUsage / num)));
                            rb.setIoUsage(Double.parseDouble(String.format("%.1f", ioUsage / num)));
                            rb.setCpuRunTimeResult(cpuRunTimeResult / num);
                            rb.setIoTimeResult(ioTimeResult / num);
                            rb.setRunTimeResult(runTimeResult / num);
                        }
                        rb.setCreateTime(System.currentTimeMillis());
                        if (null != kafkaProducer) {
                            String msg = JSON.toJSONString(rb);
                            kafkaProducer.send(new ProducerRecord<>(TOPIC, String.valueOf(System.currentTimeMillis()), msg));
                            log.info(String.format("upload data:%s", msg));
                        }
                        Object[] target = new Object[12];
                        do {
                            source = reference.get();
                            target[0] = source[0];
                            target[1] = source[1];
                            target[2] = source[2];
                            target[3] = source[3];
                            target[4] = source[4];
                            target[5] = (int) source[5] - failNum;
                            target[6] = (int) source[6] - successNum;
                            target[7] = (double) source[7] - cpuUsage;
                            target[8] = (double) source[8] - ioUsage;
                            target[9] = (long) source[9] - cpuRunTimeResult;
                            target[10] = (long) source[10] - ioTimeResult;
                            target[11] = (long) source[11] - runTimeResult;
                        } while (!reference.compareAndSet(source, target)); /* 数据上报后更新数据值 */
                    }
                } catch (Exception e) {
                    log.error("upload failure", e);
                }
            }
        }, 5, reportTime, TimeUnit.SECONDS);
        init();
        log.info("initializer success...");
    }

    void init() {
        initDiamond();
        initKafka();
    }

    void initDiamond() {
        diamondClient = new DiamondClient();
        diamondClient.setDataId(DATA_ID);
        diamondClient.setPollingIntervalTime(10);
        diamondClient.setTimeout(5000);
        diamondClient.setManagerListener(new ManagerListener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                Properties properties = new Properties();
                try {
                    properties.load(new StringReader(diamondClient.getConfig()));
                    loadConfig(properties);
                    log.info(String.format("update config:%s", configInfo));
                } catch (IOException e) {
                    log.error("error", e);
                }
            }
        });
        diamondClient.init();
        Properties properties = new Properties();
        try {
            properties.load(new StringReader(diamondClient.getConfig()));
            kafkaHost = properties.getProperty("kafkaHost");
            loadConfig(properties);
        } catch (IOException e) {
            log.error("error", e);
        }
    }

    void loadConfig(Properties properties) {
        isReport = Boolean.parseBoolean(properties.getProperty("isReport"));
        String temp = properties.getProperty("blackList");
        if (!StringUtils.isEmpty(temp)) {
            blackList = temp.split("\\,");
        }
    }

    void initKafka() {
        ClassLoader classLoader = null;
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaHost);
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1000);
            //props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            classLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(null);
            kafkaProducer = new KafkaProducer<>(props);
        } catch (Exception e) {
            log.error("error", e);
        } finally {
            if (classLoader != null) {
                Thread.currentThread().setContextClassLoader(classLoader);
            }
        }
    }
}
