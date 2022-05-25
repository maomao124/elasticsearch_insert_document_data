package mao.elasticsearch_insert_document_data;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Project name(项目名称)：elasticsearch_insert_document_data
 * Package(包名): mao.elasticsearch_insert_document_data
 * Class(类名): ElasticSearchTest
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/5/25
 * Time(创建时间)： 20:12
 * Version(版本): 1.0
 * Description(描述)： ElasticSearchTest
 * 请求：
 * <p>
 * PUT book/_doc/5
 * {
 * "name" : "java编程思想",
 * "description" : "java语言是世界第一编程语言，在软件开发领域使用人数最多。",
 * "studymodel" : "201001",
 * "price" : 68.6,
 * "timestamp" : "2022-5-25 19:11:35",
 * "pic" : "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg",
 * "tags": [ "bootstrap", "dev"]
 * }
 */

@SpringBootTest
public class ElasticSearchTest
{

    private static RestHighLevelClient client;

    @BeforeAll
    static void beforeAll()
    {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
    }


    /**
     * 同步插入，方法1
     *
     * @throws IOException IOException
     */
    @Test
    void insert() throws IOException
    {
        //构建请求
        IndexRequest indexRequest = new IndexRequest("book");
        indexRequest.id("5");

        //设置请求体

        //方法1
        String json = "{\n" +
                "   \"name\" : \"java编程思想\",\n" +
                "    \"description\" : \"java语言是世界第一编程语言，在软件开发领域使用人数最多。\",\n" +
                "    \"studymodel\" : \"201001\",\n" +
                "    \"price\" : 68.6,\n" +
                "    \"timestamp\" : \"2022-5-25 19:11:35\",\n" +
                "    \"pic\" : \"group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg\",\n" +
                "    \"tags\": [ \"bootstrap\", \"dev\"]\n" +
                "}";

        //填入到IndexRequest里
        indexRequest.source(json, XContentType.JSON);
        //设置可选参数
        //超时时间，3秒超时
        indexRequest.timeout(TimeValue.timeValueSeconds(3));
        //版本号，可以实现乐观锁
        //indexRequest.versionType(VersionType.EXTERNAL);

        //发起请求
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        //获取数据
        //获取插入的类型
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED)
        {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("创建:" + result);
        }
        else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED)
        {
            DocWriteResponse.Result result = indexResponse.getResult();
            System.out.println("更新:" + result);
        }
        else
        {
            System.out.println("其它");
        }
    }

    @Test
    void insert_async()
    {
        //构建请求
        IndexRequest indexRequest = new IndexRequest("book");
        indexRequest.id("5");
        //设置请求体

        //方法1
        String json = "{\n" +
                "   \"name\" : \"java编程思想\",\n" +
                "    \"description\" : \"java语言是世界第一编程语言，在软件开发领域使用人数最多。\",\n" +
                "    \"studymodel\" : \"201001\",\n" +
                "    \"price\" : 68.6,\n" +
                "    \"timestamp\" : \"2022-5-25 19:11:35\",\n" +
                "    \"pic\" : \"group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg\",\n" +
                "    \"tags\": [ \"bootstrap\", \"dev\"]\n" +
                "}";

        //填入到IndexRequest里
        indexRequest.source(json, XContentType.JSON);
        //发起异步请求
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
        {
            @Override
            public void onResponse(IndexResponse indexResponse)
            {
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED)
                {
                    DocWriteResponse.Result result = indexResponse.getResult();
                    System.out.println("创建:" + result);
                }
                else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED)
                {
                    DocWriteResponse.Result result = indexResponse.getResult();
                    System.out.println("更新:" + result);
                }
                else
                {
                    System.out.println("其它");
                }
            }

            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }


    /**
     * 方法2
     */
    @Test
    void insert2_async()
    {
        //构建请求
        IndexRequest indexRequest = new IndexRequest("book");
        indexRequest.id("5");
        //设置请求体

        //方法2
        Map<String, Object> map = new HashMap<>();
        map.put("name", "java编程思想");
        map.put("description", "java语言是世界第一编程语言，在软件开发领域使用人数最多。");
        map.put("studymodel", "201001");
        map.put("price", 68.6);
        map.put("timestamp", "2022-5-25 19:11:35");
        map.put("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
        map.put("tags", new String[]{"bootstrap", "dev"});

        indexRequest.source(map);

        //发起异步请求
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
        {
            @Override
            public void onResponse(IndexResponse indexResponse)
            {
                System.out.println("成功：" + indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }


    /**
     * 方法3
     *
     * @throws IOException IOException
     */
    @Test
    void insert3_async() throws IOException
    {
        //构建请求
        IndexRequest indexRequest = new IndexRequest("book");
        indexRequest.id("5");
        //设置请求体

        //方法3
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        {
            xContentBuilder.field("name", "java编程思想");
            xContentBuilder.field("description", "java语言是世界第一编程语言，在软件开发领域使用人数最多。");
            xContentBuilder.field("studymodel", "201001");
            xContentBuilder.field("price", 68.6);
            xContentBuilder.field("timestamp", "2022-5-25 19:11:35");
            xContentBuilder.field("pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg");
            xContentBuilder.field("tags", new String[]{"bootstrap", "dev"});
        }
        xContentBuilder.endObject();

        //加入到请求里
        indexRequest.source(xContentBuilder);

        //发起异步请求
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
        {
            @Override
            public void onResponse(IndexResponse indexResponse)
            {
                System.out.println("成功：" + indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 方法4
     *
     * @throws IOException IOException
     */
    @Test
    void insert4_async() throws IOException
    {
        //构建请求
        IndexRequest indexRequest = new IndexRequest("book");
        indexRequest.id("5");
        //设置请求体

        //方法4
        indexRequest.source("name", "java编程思想",
                "description", "java语言是世界第一编程语言，在软件开发领域使用人数最多。",
                "studymodel", "201001",
                "price", 69.6,
                "timestamp", "2022-5-25 19:11:35",
                "pic", "group1/M00/00/00/wKhlQFs6RCeAY0pHAAJx5ZjNDEM428.jpg",
                "tags", new String[]{"bootstrap", "dev"});

        //发起异步请求
        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
        {
            @Override
            public void onResponse(IndexResponse indexResponse)
            {
                System.out.println("成功：" + indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e)
            {
                e.printStackTrace();
            }
        });

        try
        {
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }
}
