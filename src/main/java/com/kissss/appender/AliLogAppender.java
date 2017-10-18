package com.kissss.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PutLogsRequest;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by XiaoLei on 2017/5/24.
 */
public class AliLogAppender extends AppenderBase<ILoggingEvent> {

    private Layout<ILoggingEvent> layout;

    private String host;

    //存储所有的 logItem
    private Map<Integer, LogItem> logItemMap = new ConcurrentHashMap<>();
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String projectName;
    private String logStore;
    private String topic;
    private long periodPush = 1000;
    private boolean enable;
    private String source = "";

    private Client logClient;

    //1秒钟推送一次

    private static final String LEVEL = "level";
    private static final String MSG = "_msg";
    private static final String TIME = "time";

    private static String formatDate(String formatStr) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
        return sdf.format(new Date());
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        LogItem logItem = new LogItem();
        eventObject.getMDCPropertyMap().forEach(logItem::PushBack);
        //索引
        logItem.PushBack(LEVEL, eventObject.getLevel().toString());
        logItem.PushBack(TIME, formatDate("yyyy-MM-dd HH:mm:ss.SSS"));
        logItem.PushBack(MSG, layout.doLayout(eventObject));

        logItemMap.put(logItem.hashCode(), logItem);
    }


    class PutLogsTask extends TimerTask {
        @Override
        public void run() {
            //如果为空 取消
            if (logItemMap.isEmpty()) {
                return;
            }
            Vector<LogItem> logGroup = new Vector<>();

            logItemMap.entrySet().parallelStream().forEach(entity -> {
                logGroup.add(entity.getValue());
                //删除
                logItemMap.remove(entity.getKey());
            });
            PutLogsRequest req2 = new PutLogsRequest(projectName, logStore, topic, source, logGroup);
            try {
                logClient.PutLogs(req2);
            } catch (LogException e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void start() {
        //只有存在值得情况下 才初始化
        if (endpoint.contains("_IS_UNDEFINED") || !enable) {
            return;
        }
        logClient = new Client(endpoint, accessKeyId, accessKeySecret);
        Timer timer = new Timer();
        timer.schedule(new PutLogsTask(), 0, periodPush);
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }


    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getLogStore() {
        return logStore;
    }

    public void setLogStore(String logStore) {
        this.logStore = logStore;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getPeriodPush() {
        return periodPush;
    }

    public void setPeriodPush(long periodPush) {
        this.periodPush = periodPush;
    }
}
