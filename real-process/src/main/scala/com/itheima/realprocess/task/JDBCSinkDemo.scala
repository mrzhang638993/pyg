public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //准备数据源
        List<Product> list = new ArrayList<>();
        list.add(new Product("跑鞋", "耐克", 998));
        list.add(new Product("短裤", "李宁", 119));
        list.add(new Product("袜子", "耐克", 68));
        list.add(new Product("西服", "海澜之家", 1000));
        DataStreamSource<Product> dataStreamSource = env.fromCollection(list);

        String url = "jdbc:mysql://localhost:3306/flink_data?useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT";
        String sql = "insert into t_product(name, brand, price) values (?,?,?)";
        dataStreamSource.addSink(JdbcSink.sink(sql,
                new JdbcStatementBuilder<Product>() {
                    @Override
                    public void accept(PreparedStatement ps, Product product) throws SQLException {
                        ps.setString(1, product.getName());
                        ps.setString(2, product.getBrand());
                        ps.setDouble(3, product.getPrice());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        env.execute("jdbc data sink");
    }
