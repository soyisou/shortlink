package com.acjava.shortlink.project.toolkit;

import com.nageoffer.shortlink.project.toolkit.LinkUtil;
import org.junit.Test;

/**
 * ClassName: LinkUtilTest
 * Description:
 *
 * @author 87866
 * @date 2024/5/16 上午10:59
 */
public class LinkUtilTest {
    @Test
    public void testLinkUtil(){
        long linkCacheValidTime = LinkUtil.getLinkCacheValidTime(null);
        System.out.println(linkCacheValidTime);
    }
}
