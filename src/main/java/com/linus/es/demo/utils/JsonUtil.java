package com.linus.es.demo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URL;

/**
 * @author yuxuecheng
 * @Title: JsonUtil
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/21 10:51
 */
@Slf4j
public class JsonUtil {
    private JsonUtil() {

    }

    //filename 为文件名字 如 “/json/app_version_info.json”

    /**
     *
     * @param filename json文件路径，相对于resources目录的相对路径，例如/json/index_properties.json
     * @return
     */
    public static JSONObject getJsonObjFromResource(String filename) {
        JSONObject json = null;

        if (!filename.endsWith(".json")) {
            filename += ".json";
        }

        try {
            URL url = JsonUtil.class.getResource(filename);
            String path = url.getPath();
            File file = new File(path);
            if (file.exists()) {
                String content = FileUtils.readFileToString(file, "UTF-8");
                json = JSON.parseObject(content);
            } else {
                log.warn("file not exist!");
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.info("readFileToString" + e.getMessage());
        }

        return json;
    }
}
