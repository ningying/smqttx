package io.github.quickmsg.common.log;

import cn.hutool.core.collection.ConcurrentHashSet;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import io.github.quickmsg.common.channel.MqttChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Optional;

/**
 * @author luxurong
 */
@Slf4j
public class LogManager {
    public volatile static boolean logAll = true;

    private final ConcurrentHashSet<String> debugClientIds = new ConcurrentHashSet<>();

    /**
     * 节点IP
     */
    private final String nodeIp;

    public LogManager(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public void addDebugClient(String client) {
        debugClientIds.add(client);
    }

    public void removeDebugClient(String client) {
        debugClientIds.remove(client);
    }

    public void printInfo(MqttChannel mqttChannel, LogEvent type, LogStatus eventStatus, String message) {
        if (logAll) {
            log.info("{}|{}|{}|{}|{}|{}",
                    nodeIp,
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getAddress).orElse(null),
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getClientId).orElse(null),
                    type.getName(),
                    eventStatus.getName(),
                    this.toTruncateFieldStr(message));
        }
    }

    /**
     * error日志打印
     */
    public void printError(MqttChannel mqttChannel, LogEvent type, String message) {
        if (logAll) {
            log.info("{}|{}|{}|{}|{}|{}",
                    nodeIp,
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getAddress).orElse("system"),
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getClientId).orElse("system"),
                    type.getName(),
                    LogStatus.FAILED.getName(),
                    this.toTruncateFieldStr(message));
        }
    }



    /**
     * warn日志打印
     */
    public void printWarn(MqttChannel mqttChannel, LogEvent type, LogStatus logStatus,String message) {
        if (logAll){
            log.info("{}|{}|{}|{}|{}|{}",
                    nodeIp,
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getAddress).orElse(null),
                    Optional.ofNullable(mqttChannel).map(MqttChannel::getClientId).orElse(null),
                    type.getName(),
                    logStatus.getName(),
                    this.toTruncateFieldStr(message));
        }
    }

    private String toTruncateFieldStr(String msg) {
        if(msg == null) return "";
        JSONObject jsonObject = JSONObject.parseObject(msg);
        SerializeConfig serializeConfig = new SerializeConfig();
        FieldTruncatingSerializer fieldTruncatingSerializer = new FieldTruncatingSerializer();
        serializeConfig.put(String.class, fieldTruncatingSerializer);
        return JSON.toJSONString(jsonObject, serializeConfig);
    }

    /**
     * json序列化截断器
     */
    static class FieldTruncatingSerializer implements ObjectSerializer {

        private int maxLength = 1000;

        public FieldTruncatingSerializer() {

        }
        public FieldTruncatingSerializer(int maxLength) {
            this.maxLength = maxLength;
        }

        @Override
        public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
            String value = (String) object;
            if (value.length() > maxLength) {
                serializer.write(value.substring(0, maxLength).concat("..."));
            } else {
                serializer.write(value);
            }
        }
    }


}
