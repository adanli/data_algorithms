package demo

import config.SparkConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

object SparkExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkConfig.spark("spark-example")

    /*val tables = Seq("t_agn_unsettlement_history", "t_agn_report_history", "t_claim_info")

    for (table <- tables) {
      spark.read.format("jdbc")
        .option("url", "jdbc:mysql://11.0.0.199:3339/claim_unsettlement?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=true&serverTimezone=Asia/Shanghai&nullCatalogMeansCurrent=true")
        .option("dbtable", table)
        .option("user", "root")
        .option("password", "123456")
        .load()
        .createOrReplaceTempView(table)
    }

    */

//    spark.udf.register("castToInteger", (s: String) => s.toInt)

    val schema_unsettlement =
      """
        |  `unsettlement_id` String,
        |  `claim_apply_no` String ,
        |  `claim_by_policy_no` String ,
        |  `clause_code` String ,
        |  `clause_name` String ,
        |  `coverage_code` String ,
        |  `coverage_name` String ,
        |  `item_code` String ,
        |  `unsettlement_amount` Double ,
        |  `average_amount` Double ,
        |  `phase_cd` String ,
        |  `unsettlement_dt` Date,
        |  `version` Int ,
        |  `need_man_made` String ,
        |  `currency` String ,
        |  `is_valid` Int ,
        |  `gmt_create` Date ,
        |  `gmt_modified` Date ,
        |  `gmt_delete` String ,
        |  `creator_name` String ,
        |  `creator` String ,
        |  `modifier_name` String ,
        |  `modifier` String ,
        |  `flag` String ,
        |  `unsettlement_modified` Date ,
        |  `trace_id` String ,
        |  `unsettle_code` String ,
        |  `trace_oss_id` String ,
        |  `coinsurance_unsettle_amount` Double
        |""".stripMargin
        
    
    val schema_report =
      """
        |  `report_id` String,
        |  `claim_apply_no` String,
        |  `report_info_code` String,
        |  `report_dt` Date,
        |  `reporter_code` String,
        |  `driver_code` String,
        |  `driver_insured_association_cd` String,
        |  `phone_access_dt` Date,
        |  `phone_finish_dt` Date,
        |  `receiver_code` String,
        |  `receiver_name` String,
        |  `currency` String,
        |  `report_loss_amount` Double,
        |  `third_report_no` String,
        |  `third_report_organization` String,
        |  `tenant_code` String,
        |  `gmt_create` Date,
        |  `gmt_modified` Date,
        |  `is_valid` Int,
        |  `branch_code` String,
        |  `is_latest` String,
        |  `version` Int,
        |  `flag` String,
        |  `modifier` String,
        |  `modifier_name` String,
        |  `creator` String,
        |  `creator_name` String,
        |  `report_record_id` String,
        |  `report_phone_no` String,
        |  `reporter_and_insured_rel_cd` String,
        |  `report_source_cd` String,
        |  `report_accept_channel_cd` String,
        |  `acceptance_organization` String,
        |  `create_version` Int,
        |  `del_version` Int,
        |  `is_frozen` Int,
        |  `com_path` String,
        |  `is_confused` String
        |
        |""".stripMargin


    val schema_claim_info =
      """
        |  `claim_no` String,
        |  `claim_status` String,
        |  `out_company_code` String,
        |  `out_company_name` String,
        |  `out_branch_code` String,
        |  `out_branch_name` String,
        |  `claim_time` Date,
        |  `claim_finish_time` String,
        |  `policy_no` String,
        |  `policy_out_company_code` String,
        |  `policy_out_company_name` String,
        |  `product_code` String,
        |  `product_name` String,
        |  `product_big_code` String,
        |  `product_big_name` String,
        |  `product_small_code` String,
        |  `product_small_name` String,
        |  `claim_apply_no` String,
        |  `latest_case_type` String,
        |  `survey_case_type` String,
        |  `case_status` String,
        |  `claim_estimate_loss_amount` String,
        |  `case_estimate_loss_amount` String,
        |  `claim_paid_amount` String,
        |  `case_paid_amount` String,
        |  `compulsory_paid_amount` String,
        |  `car_damage_paid_amount` String,
        |  `third_party_paid_amount` String,
        |  `theft_paid_amount` String,
        |  `passenger_paid_amount` String,
        |  `other_paid_amount` String,
        |  `paid_amount` String,
        |  `last_paid_time` String,
        |  `report_time` String,
        |  `finish_time` String,
        |  `case_manager_no` String,
        |  `case_manager_name` String,
        |  `case_manager_phone` String,
        |  `case_manager_out_company_code` String,
        |  `case_manager_out_company_name` String,
        |  `accident_type` String,
        |  `accident_address` String,
        |  `accident_longitude` String,
        |  `accident_latitude` String,
        |  `accident_reason` String,
        |  `is_single_car_accident` String,
        |  `accident_responsibility_type` String,
        |  `accident_responsibility_ratio` String,
        |  `risk_level` String,
        |  `risk_score` String,
        |  `reopen_times` String,
        |  `car_damage_num` String,
        |  `car_damage_num_target` String,
        |  `third_party_damage_num` String,
        |  `person_damage_num` String,
        |  `property_damage_num` String,
        |  `policy_start_time` String,
        |  `policy_end_time` String,
        |  `policy_holder` String,
        |  `driving_license_owner` String,
        |  `accident_driver` String,
        |  `is_valid_report` String,
        |  `accident_time` String,
        |  `survey_time` String,
        |  `report_person` String,
        |  `report_phone` String,
        |  `report_person_accept` String,
        |  `report_person_accept_account` String,
        |  `car_no` String,
        |  `engine_no` String,
        |  `car_frame_no` String,
        |  `car_color` String,
        |  `seat_num` String,
        |  `load_ton` String,
        |  `first_register_time` String,
        |  `vin_code` String,
        |  `brand_model` String,
        |  `car_type` String,
        |  `use_type` String,
        |  `business_type` String,
        |  `channel_name` String,
        |  `channel_type` String,
        |  `business_source` String,
        |  `service_code_type` String,
        |  `service_code` String,
        |  `agent` String,
        |  `salesman` String,
        |  `third_party_car_no` String,
        |  `is_first_scene_survey` String,
        |  `is_other_place_survey` String,
        |  `first_assign_case_manager_no` String,
        |  `first_assign_case_manager_name` String,
        |  `first_assign_case_manager_out_company_code` String,
        |  `first_assign_case_manager_out_company_name` String,
        |  `finish_case_manager_no` String,
        |  `finish_case_manager_name` String,
        |  `finish_case_manager_out_company_code` String,
        |  `finish_case_manager_out_company_name` String,
        |  `is_double_accident` String,
        |  `is_involve_lawsuit` String,
        |  `is_theft_case` String,
        |  `is_severe_wind_case` String,
        |  `is_involve_death` String,
        |  `is_involve_disable` String,
        |  `add_service_type` String,
        |  `pay_times` String,
        |  `insurance_amount` String,
        |  `actual_premium` String,
        |  `new_car_purchase_price` String,
        |  `claim_amount` String,
        |  `direct_fee_amount` String,
        |  `claim_item_type` String,
        |  `if_subject` String,
        |  `deductible` String,
        |  `claim_mode` String,
        |  `if_city_branch` String,
        |  `occupation_type` String,
        |  `procedure_fee` String,
        |  `underwriter_no` String,
        |  `underwriter_name` String,
        |  `underwriter_start_time` String,
        |  `underwriter_end_time` String,
        |  `underwriter_type` String,
        |  `all_online` String,
        |  `doc_collect` String,
        |  `doc_collect_first` String,
        |  `pay_first_dt` String,
        |  `pay_check_dt` String
        |
        |""".stripMargin


    spark.read.schema(schema_unsettlement).csv("s3a://data/系统清单数据/unsettle/unsettle/unsettlement/claim_unsettlement_t_unsettlement_history.csv").createOrReplaceTempView("t_agn_unsettlement_history")
    spark.read.schema(schema_report).csv("s3a://data/系统清单数据/unsettle/unsettle/report/t_agn_report_history.csv").createOrReplaceTempView("t_agn_report_history")
    spark.read.schema(schema_claim_info).csv("s3a://data/系统清单数据/unsettle/unsettle/claim_by_policy/claim_unsettlement_t_claim_info.csv").createOrReplaceTempView("t_claim_info")


//    spark.sql("select * from t_agn_unsettlement_history").show()
//    spark.sql("select * from t_agn_report_history").show()
//    spark.sql("select * from t_claim_info").show()


    spark.sql(
      """
        |select
        |    origin.claim_apply_no as `索赔申请号`,
        |    report.report_dt as `报案时间`,
        |    claim.sec_comname as `出单分公司`,
        |    origin.unsettlement_modified as `惩罚性赋值发生时间`,
        |    origin.unsettlement_amount as `惩罚性赋值后的金额`,
        |    pre.unsettlement_amount as `惩罚性赋值前的金额`,
        |    if(current_unsettle.is_punish = 1, '是', '否') as `最新赋值金额是否惩罚性赋值`
        |    from (
        |
        |        select unsettle.claim_apply_no, unsettle_039.trace_id, sum(unsettle.unsettlement_amount) as unsettlement_amount, min(unsettlement_modified_039) as unsettlement_modified from (
        |            select claim_apply_no, claim_by_policy_no, unsettlement_amount, unsettlement_modified, version from t_agn_unsettlement_history
        |                      ) as unsettle
        |        join (select claim_apply_no,
        |                     claim_by_policy_no,
        |                     trace_id,
        |                     max(version) as version,
        |                     min(unsettlement_modified_039) as unsettlement_modified_039
        |              from (select unsettle.*,
        |                           second(unsettle_039.unsettlement_modified) as unsettlement_modified_039, unsettle_039.trace_id
        |                    from (select claim_apply_no,
        |                                 claim_by_policy_no,
        |                                 clause_code,
        |                                 coverage_code,
        |                                 unsettlement_modified,
        |                                 unsettlement_amount,
        |                                 version,
        |                                 phase_cd
        |                          from t_agn_unsettlement_history) as unsettle
        |                             join (select claim_apply_no, unsettlement_modified, version, claim_by_policy_no, trace_id
        |                                   from t_agn_unsettlement_history
        |                                   where phase_cd = 'S039'
        |                                   group by claim_apply_no, unsettlement_modified, version, claim_by_policy_no, trace_id) as unsettle_039
        |                                  on unsettle.claim_apply_no =
        |                                     unsettle_039.claim_apply_no and
        |                                     unsettle.unsettlement_modified <=
        |                                     unsettle_039.unsettlement_modified
        |                                      and if(unsettle.claim_by_policy_no = unsettle_039.claim_by_policy_no,
        |                                             unsettle.version <= unsettle_039.version, 1 = 1)) as unsettle
        |              group by unsettle.claim_apply_no, unsettle.claim_by_policy_no,
        |                       trace_id) as unsettle_039 on unsettle.claim_apply_no = unsettle_039.claim_apply_no and unsettle.claim_by_policy_no = unsettle_039.claim_by_policy_no and unsettle.version = unsettle_039.version
        |        group by unsettle.claim_apply_no, trace_id
        |
        |              ) as origin
        |join (
        |
        |    select unsettle.claim_apply_no, unsettle_039.trace_id, sum(unsettle.unsettlement_amount) as unsettlement_amount from (
        |    select claim_apply_no, claim_by_policy_no, unsettlement_amount, unsettlement_modified, version from t_agn_unsettlement_history
        |              ) as unsettle
        |        join (
        |
        |            select * from (select row_number() over (partition by claim_apply_no, claim_by_policy_no, trace_id order by unsettlement_modified_039) rn,
        |                                  version.*
        |                           from (select claim_apply_no,
        |                                        claim_by_policy_no,
        |                                        unsettlement_modified_039,
        |                                        if(phase_cd = 'S039', version - 1, version) as version,
        |                                        trace_id,
        |                                        phase_cd
        |                                 from (select row_number() over (partition by claim_apply_no, claim_by_policy_no, unsettlement_modified_039, trace_id order by priority desc , version desc, priority desc ) rn,
        |                                              unsettle.*
        |                                       from (select unsettle.*,
        |                                                    second(unsettle_039.unsettlement_modified) as unsettlement_modified_039,
        |                                                    unsettle_039.trace_id,
        |                                                    if(unsettle.phase_cd = 'S039', 1, 0)                                 as priority
        |                                             from (select claim_apply_no,
        |                                                          claim_by_policy_no,
        |                                                          clause_code,
        |                                                          coverage_code,
        |                                                          unsettlement_modified,
        |                                                          unsettlement_amount,
        |                                                          version,
        |                                                          phase_cd
        |                                                   from t_agn_unsettlement_history) as unsettle
        |                                                      join (select claim_apply_no,
        |                                                                   unsettlement_modified,
        |                                                                   trace_id,
        |                                                                   claim_by_policy_no,
        |                                                                   version
        |                                                            from t_agn_unsettlement_history
        |                                                            where phase_cd = 'S039'
        |                                                            group by claim_apply_no,
        |                                                                     unsettlement_modified,
        |                                                                     trace_id,
        |                                                                     claim_by_policy_no,
        |                                                                     version) as unsettle_039
        |                                                           on unsettle.claim_apply_no =
        |                                                              unsettle_039.claim_apply_no and
        |                                                              unsettle.unsettlement_modified <=
        |                                                              unsettle_039.unsettlement_modified
        |                                                               and
        |                                                              if(
        |                                                                          unsettle.claim_by_policy_no =
        |                                                                          unsettle_039.claim_by_policy_no,
        |                                                                          unsettle.version <=
        |                                                                          unsettle_039.version,
        |                                                                          1 = 1)) as unsettle) as unsettle
        |                                 where rn = 1) as version) as version where rn = 1
        |
        |            ) as unsettle_039 on unsettle.claim_apply_no = unsettle_039.claim_apply_no and unsettle.claim_by_policy_no = unsettle_039.claim_by_policy_no and unsettle.version = unsettle_039.version
        |    group by unsettle.claim_apply_no, trace_id
        |
        |) as pre on origin.claim_apply_no = pre.claim_apply_no and origin.trace_id = pre.trace_id
        |join (
        |    select claim_apply_no, min(report_dt) as report_dt from t_agn_report_history
        |             group by claim_apply_no
        |    ) as report on origin.claim_apply_no = report.claim_apply_no
        |join (
        |    select claim_apply_no, concat_ws(',', collect_set(out_company_name)) as sec_comname from t_claim_info
        |             group by claim_apply_no
        |) as claim on origin.claim_apply_no = claim.claim_apply_no
        |left join (
        |    select claim_apply_no, if(phase_cd = 'S039', 1, 0) as is_punish from (select row_number() over (partition by claim_apply_no order by unsettlement_modified desc ) rn,
        |                      unsettle.*
        |               from (select claim_apply_no,
        |                            claim_by_policy_no,
        |                            version,
        |                            phase_cd,
        |                            unsettlement_modified,
        |                            if(phase_cd = 'S039', 1, 0) as priority
        |                     from t_agn_unsettlement_history) as unsettle) as unsettle where rn = 1
        |    ) as current_unsettle on origin.claim_apply_no = current_unsettle.claim_apply_no
        |    where origin.claim_apply_no = 'R230920C00000010000764547'
        |
        |""".stripMargin).createOrReplaceTempView("t_result")
//      .show()
//      .write.csv("s3a://data/系统清单数据/unsettle/unsettle/result/punish_amount")

    spark.sql("select * from t_result where `索赔申请号` = 'R230903C00000010000808310'")
      .show()

  }

}
