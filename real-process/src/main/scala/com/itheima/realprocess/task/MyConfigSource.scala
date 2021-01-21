public class MyConfigSource implements SourceFunction<String> {
    private boolean isRunning = true;
    String confPath = "F:\\data\\conf.txt";

    /**
     * run方法里编写数据产生逻辑
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            List<String> lines = Files.readAllLines(Paths.get(confPath));
            lines.forEach(ctx::collect);
            Thread.sleep(30000);//每隔30秒更新一次配置
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
