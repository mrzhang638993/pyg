public class UVdemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //kafka配置参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.174.129:9092");
        props.put("group.id", "web-uv-stat");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(
                "uv_statistics",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props);

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig
                .Builder().setHost("127.0.0.1").setPort(6379).build();

        env.addSource(kafkaConsumer011)
                .setParallelism(2)
                .map(str -> {
                    //解析日志时间戳，转换为日期
                    Map<String, String> o = JSON.parseObject(str, Map.class);
                    String timestamp = o.get("timestamp");
                    Instant instant = Instant.ofEpochMilli(Long.parseLong(timestamp));
                    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                    UserVisitEvent userVisitEvent = JSON.parseObject(str, UserVisitEvent.class);
                    userVisitEvent.date = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                    return userVisitEvent;
                })
                .keyBy("date", "pageId")//按照日期和页面id分组，每个页面用独立的KeyedState来存储数据
                .map(new RichMapFunction<UserVisitEvent, Tuple2<String, Long>>() {
                    // 存储userId集合，使用MapState存储key即可，value不使用
                    private MapState<String, String> userIdSet;
                    // MapState没有统计元素个数的方法，所以需要一个存储UV值的状态
                    private ValueState<Long> uvCount;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 从状态中恢复userIdSet状态
                        userIdSet = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("userIdSet",
                                        TypeInformation.of(new TypeHint<String>() {
                                        }),
                                        TypeInformation.of(new TypeHint<String>() {
                                        })));
                        // 从状态中恢复uvCount状态
                        uvCount = getRuntimeContext().getState(new ValueStateDescriptor<>("uvCount", TypeInformation.of(new TypeHint<Long>() {
                        })));
                    }

                    @Override
                    public Tuple2<String, Long> map(UserVisitEvent userVisitEvent) throws Exception {
                        // 设置uvCount初始值
                        if (null == uvCount.value()) {
                            uvCount.update(0L);
                        }
                        if (!userIdSet.contains(userVisitEvent.uid)) {
                            userIdSet.put(userVisitEvent.uid, null);
                            //用户首次访问才更新uvCount计数
                            uvCount.update(uvCount.value() + 1);
                        }
                        // 生成写入redis的数据，格式:key=date_pageId,value=count值
                        String redisKey = userVisitEvent.date + "_"
                                + userVisitEvent.pageId;
                        System.out.println(redisKey + ":" + uvCount.value());
                        return Tuple2.of(redisKey, uvCount.value());
                    }
                })
                .addSink(new RedisSink<>(redisConf, new RedisSinkMapper()));

        env.execute("UV Statistics");
    }

    private static class RedisSinkMapper implements RedisMapper<Tuple2<String, Long>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return data.f1.toString();
        }
    }
}
