public class BroadCastDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //kafka配置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.174.136:9092");
        props.put("zookeeper.connect", "192.168.174.136:2181");
        //props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        //定义MapStateDescriptor来保存要广播的数据
        final MapStateDescriptor<String, Double> CONFIG_DESCRIPTOR = new MapStateDescriptor<>(
                "speed_config",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.DOUBLE_TYPE_INFO);

        //配置数据流，读取配置
        BroadcastStream<String> broadcastStream = env.addSource(new FlinkKafkaConsumer<>(
                "city_speed_config",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).broadcast(CONFIG_DESCRIPTOR);

        //业务数据流，处理数据
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "driver_upload",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props));

        KeyedStream<DriverUploadInfo, String> driverStream = dataStreamSource.map(new MapFunction<String, DriverUploadInfo>() {
            @Override
            public DriverUploadInfo map(String value) throws Exception {
                DriverUploadInfo driverUploadInfo = JSON.parseObject(value, DriverUploadInfo.class);
                return driverUploadInfo;
            }
        }).keyBy(new KeySelector<DriverUploadInfo, String>() {
            @Override
            public String getKey(DriverUploadInfo value) throws Exception {
                return value.driverId;
            }
        });

        driverStream.connect(broadcastStream).process(new KeyedBroadcastProcessFunction<Object, DriverUploadInfo, String, Object>() {
            //使用ValueState保存上一次司机状态信息
            private transient ValueState<DriverUploadInfo> driverState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<DriverUploadInfo> descriptor = new ValueStateDescriptor<>("driverInfoState", DriverUploadInfo.class);
                driverState = getRuntimeContext().getState(descriptor);//注册状态
            }

            @Override
            public void processElement(DriverUploadInfo driverUploadInfo, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                System.out.println("received event:" + driverUploadInfo);
                ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                Double speedLimit = broadcastState.get(driverUploadInfo.cityCode);

                DriverUploadInfo previousInfo = driverState.value();
                if (previousInfo != null && speedLimit != null) {
                    //计算平均车速，通过里程表数差除以时间戳之差求得这段时间内的平均速度
                    double distance = driverUploadInfo.totalMileage - previousInfo.totalMileage;
                    double interval = (driverUploadInfo.timestamp - previousInfo.timestamp) / 1000;
                    double speed = distance / interval;
                    System.out.println("current speedLimit:" + speedLimit + ",current speed:" + speed);
                    if (speed > speedLimit) {
                        out.collect(new Tuple2<String, Double>(driverUploadInfo.driverId + "已超速", speed));
                    }
                }
                //更新状态
                driverState.update(driverUploadInfo);
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println("update config:" + value);
                List<CityConfig> cityConfigs = JSON.parseArray(value, CityConfig.class);
                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                cityConfigs.forEach(e -> {
                    try {
                        broadcastState.put(e.cityCode, e.speedLimit);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
            }
        }).print();

        env.execute("Broadcast State demo");
    }
}
