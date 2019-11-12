package com.linus.es.demo.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.linus.es.demo.dao.BaseElasticDao;
import com.linus.es.demo.entity.ElasticEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yuxuecheng
 * @Title: ImportDataToEsFromExcel
 * @ProjectName demo
 * @Description: TODO
 * @date 2019/11/11 19:08
 */
@Slf4j
@Component
@Order(value = 2)
public class ImportDataToEsFromExcel implements CommandLineRunner {

    @Autowired
    private BaseElasticDao elasticDao;

    private static List<String> BASIC_INFO_COLUMNS = Arrays.asList("hospital_name",
            "pay_type",
            "admission_num",
            "inpatient_id",
            "inpatient_serial_num",
            "name",
            "sex",
            "birth_date",
            "age",
            "marital_status",
            "profession",
            "birthplace",
            "nationality",
            "country",
            "identity_card_num",
            "present_addr",
            "home_telephone",
            "mobile",
            "work_unit_addr",
            "registered_addr",
            "contact_name",
            "relationship",
            "contact_addr",
            "contact_mobile",
            "admission_type",
            "admission_date",
            "admission_dep",
            "admission_ward",
            "transfer_dep",
            "discharge_date",
            "discharge_dep",
            "discharge_ward",
            "hospital_stay",
            "admission_status",
            "section_director",
            "chief_physician",
            "attending_physician",
            "resident",
            "primary_nurse",
            "medical_record_quality",
            "chief_complaint",
            "present_illness_history",
            "past_medical_history",
            "personal_history",
            "marriage_history",
            "menstrual_history",
            "family_history",
            "physical_examination",
            "auxiliary_examination",
            "medical_history_summary",
            "primary_diagnosis",
            "first_disease_course",
            "daily_disease_course",
            "operation_record",
            "salvage_record",
            "discharge_record",
            "discharge_abstract",
            "death_record");

    private static List<String> DIAGNOSIS_DATA_COLUMNS = Arrays.asList("diagnosis_code", "diagnosis_name");

    private static List<String> OPERATION_DATA_COLUMNS = Arrays.asList("operation_code", "operation_date", "operation_name", "operator");

    private static List<String> TREATMENT_DATA_COLUMNS = Arrays.asList("inpatient_serial_num", "treatment_date", "treatment_name", "treatment_category_name", "total_dose", "total_dose_unit", "per_dose", "per_dose_unit");

    private static List<String> CHECK_DATA_COLUMNS = Arrays.asList("inpatient_serial_num", "check_date", "check_item_name", "check_detail_item_name", "check_result", "check_result_unit", "reference_value");

    private static List<String> EXAMINATION_DATA_COLUMNS = Arrays.asList("inpatient_serial_num", "examination_category_code", "image_direction", "examination_result");
//    private static List<String> EXAMINATION_DATA_COLUMNS = Arrays.asList("inpatient_serial_num", "examination_category_code", "image_description", "examination_result");

    private static List<String> ALL_INFO_COLUMNS = new ArrayList<>();

    static {
        Collections.addAll(ALL_INFO_COLUMNS, BASIC_INFO_COLUMNS.toArray(new String[0]));
        Collections.addAll(ALL_INFO_COLUMNS, "diagnosis_data", "operation_data", "treatment_data",
                "check_data", "examination_data");
    }

    public void importData(String fileName, String sheetName) {
        List<String> columns = new ArrayList<>();
        Set<String> dateColumns = new HashSet<>(Arrays.asList("admission_date", "discharge_date", "operation_date", "date"));
        Set<String> nestedColumns = new HashSet<>(Arrays.asList("check_data", "diagnosis_data", "examination_data",
                "operation_data", "treatment_data"));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        SimpleDateFormat targetDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String defaultDateStr = "1970-01-01 00:00:00";
        log.info(System.getProperty("user.dir"));
        try {
            // 创建Excel表
            XSSFWorkbook book = new XSSFWorkbook(fileName);
            // 在当前Excel创建一个子表
            XSSFSheet sheet = book.getSheet(sheetName);
            if (sheet == null) {
                log.warn("指定的sheet页不存在：" + sheetName);
                return;
            }
            Row firstRow = sheet.getRow(0);
            for (Cell cell : firstRow) {
                columns.add(cell.getStringCellValue());
            }
            List<ElasticEntity> elasticEntityList = new ArrayList<>();
            for (Row row : sheet) {
                if (row.getRowNum() == 0) {
                    continue;
                }
                Map<String, Object> data = new ConcurrentHashMap<>();
                String id = UUID.randomUUID().toString();
                data.put("id", id);
                for (int i = 0; i < columns.size(); i++) {
                    String columnName = columns.get(i);
                    Cell cell = row.getCell(i);
                    if (cell == null) {
                        log.error("列名：" + columnName + "不存在");
                        if (nestedColumns.contains(columnName)) {
                            data.put(columnName, JSONArray.parseArray("[]"));
                        } else {
                            data.put(columnName, "");
                        }
                        continue;
                    }
                    String value = cell.getStringCellValue();
                    log.info("列名：" + columnName + ", 值：" + value);
                    if (dateColumns.contains(columnName)) {
                        try {
                            Date date = simpleDateFormat.parse(value);
                            String targetDateStr = targetDateFormat.format(date);
//                            long milliSeconds = date.getTime();
                            data.put(columnName, targetDateStr);
                        } catch (ParseException pe) {
                            log.error(pe.getMessage(), pe);
                            data.put(columnName, defaultDateStr);
                        }
                    } else if (nestedColumns.contains(columnName)) {
                        value = StringUtils.replace(value, "--", "");
                        JSONArray jsonArray = JSON.parseArray(value);
                        if (StringUtils.isEmpty(value)) {
                            jsonArray = JSON.parseArray("[]");
                        }
                        data.put(columnName, jsonArray);
                    } else {
                        data.put(columnName, value);
                    }
                }
                ElasticEntity elasticEntity = new ElasticEntity();
                elasticEntity.setId(id);
                elasticEntity.setData(data);
                elasticDao.insertOrUpdateOne("original", elasticEntity);
                elasticEntityList.add(elasticEntity);
//                if (elasticEntityList.size() >= 10) {
//                    elasticDao.insertBatch("original", elasticEntityList);
//                    elasticEntityList.clear();
//                }
            }
        } catch (IOException ioe) {
            log.error("文件不存在：" + fileName);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("开始数据导入");
        String fileName = "住院数据_ES.xlsx";
        String sheetName = "住院数据";
        importData(fileName, sheetName);
        log.info("数据导入完成");
    }
}
