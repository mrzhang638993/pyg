public static void main(String[] args) throws Exception {
        int port = 9001;
        String hostname = "192.168.174.136";
        String delimiter = "\n";
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //连接socket获取输入数据
        DataStreamSource<String> data = environment.socketTextStream(hostname, port, delimiter);

        //字符串数据转换为实体类
        data.map(new MapFunction<String, DriverMileages>() {
            @Override
            public DriverMileages map(String value) throws Exception {
                String[] split = value.split(",");
                DriverMileages driverMileages = new DriverMileages();
                driverMileages.driverId = split[0];
                driverMileages.currentMileage = Double.parseDouble(split[1]);
                driverMileages.timestamp = Long.parseLong(split[2]);
                return driverMileages;
            }
        })
                .keyBy(DriverMileages::getDriverId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            	//.timeWindow(Time.seconds(5))
                .sum("currentMileage")
                .print();

        //启动计算任务
        environment.execute("window demo1");
    }
