package cn.itcast.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@MapperScan("cn.itcast.hotel.mapper")
@SpringBootApplication
public class HotelDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(HotelDemoApplication.class, args);
    }

    @Bean
    public ElasticsearchClient restHighLevelClient(){
        RestClient restClient = RestClient.builder(
                new HttpHost("192.168.93.132", 9200)
        ).build();
        // 创建一个 Transport 和 JacksonJsonpMapper 序列化实例
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        // 创建 Elasticsearch 客户端
        ElasticsearchClient client = new ElasticsearchClient(transport);
        return client;
    }
}
