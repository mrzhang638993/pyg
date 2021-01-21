public class BroadCastDemo{
    public static void main(String[] args) throws Exception {
        int port = 9001;
        String hostname = "192.168.174.136";
        String delimiter = "\n";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //定义MapStateDescriptor来保存要广播的数据
        final MapStateDescriptor<String, String> CONFIG_DESCRIPTOR = new MapStateDescriptor<>(
                "config",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        //创建广播流，向下游广播配置数据
        BroadcastStream<String> broadcastStream = env.addSource(new MyConfigSource()).broadcast(CONFIG_DESCRIPTOR);
        //创建业务数据流
        //连接socket获取输入数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port, delimiter);
        dataStreamSource.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, Object>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                //获取广播数据
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                String flag = broadcastState.get(value);
                //如果key对应的value为1，则表示该数据要被处理
                if (flag != null && flag.equals("1")) {
                    out.collect(value + "被处理了");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Object> out) throws Exception {
                System.out.println("update config:" + value);
                String[] split = value.split("=");
                String key = split[0];
                String flag = split[1];
                //接收广播数据，更新State
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(CONFIG_DESCRIPTOR);
                broadcastState.put(key, flag);
            }
        }).print();

        env.execute("broadcast");
    }
}
