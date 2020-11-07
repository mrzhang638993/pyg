package com.itheima.canal.kafka.util;

import java.util.ResourceBundle;

public class GlobalConfigUtil {
    // 获取一个资源加载器
    // 资源加载器会自动去加载CLASSPATH中的application.properties配置文件
    private static final ResourceBundle resourceBundle = ResourceBundle.getBundle("application");
    // 使用ResourceBundle.getString方法来读取配置
    public static String canalHost = resourceBundle.getString("canal.host");
    public static String canalPort = resourceBundle.getString("canal.port");
    public static String canalInstance = resourceBundle.getString("canal.instance");
    public static String mysqlUsername = resourceBundle.getString("mysql.username");
    public static String mysqlPassword = resourceBundle.getString("mysql.password");
    public static String kafkaBootstrapServers = resourceBundle.getString("kafka.bootstrap.servers");
    public static String kafkaZookeeperConnect = resourceBundle.getString("kafka.zookeeper.connect");
    public static String kafkaInputTopic = resourceBundle.getString("kafka.input.topic");
}
