package com.acjava.shortlink.project.service;

import com.nageoffer.shortlink.project.service.impl.UrlTitleServiceImpl;
import org.aspectj.weaver.ast.Var;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

/**
 * ClassName: UrlTitleServiceTest
 * Description:
 *
 * @author 87866
 * @date 2024/5/16 上午9:53
 */
public class UrlTitleServiceTest {
    @Test
    public void getTitleByUrlTest() throws IOException {
        String url = "https://zhihu.com";
        URL targetUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            Document document = Jsoup.connect(url).get();
            //获取整个文档
            System.out.println(document);
            //获取网站标题
            System.out.println(document.title());
        }
        System.out.println("Error while fetching title.");
    }
}
