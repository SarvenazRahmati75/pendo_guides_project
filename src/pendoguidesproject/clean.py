import logging
import requests
from typing import Optional
import time
import pandas as pd
import boto3
from authlib.integrations.requests_client import OAuth2Session
from pendoguidesproject.secrets import Secrets
from pendoguidesproject.config import SecretsConfig
from pendoguidesproject.jobs import entrypoint
from pendoguidesproject.Redshift import redshiftExporter
import io
from io import StringIO
import os
import psycopg2
from datetime import datetime
import pytz


@entrypoint("clean") # change name to process, put in master
def run(env: str, date: str):
    os.environ["environment"] = env
    # Sleep timer
    time.sleep(5)
    # Create activity data
    completeLeft, status, pendo_usage, assets = createActivityData()
    # Create and export usage data
    createUsageData(completeLeft, pendo_usage, assets, status)
    # Complete extra ingestions
    ingestUniversity()
    ingestAllCampaigns()

def createUsageData(completeLeft, pendo_usage, assets, status):
    # Merge with pendo usage
    usage = pd.merge(completeLeft, pendo_usage, left_on="usr_id", right_on="pu_visitorid", how="inner")
    # Merge with asset names
    usage = pd.merge(usage, assets, left_on="pu_parameter", right_on="as_asset_id", how="left")
    # Add status
    usage = pd.merge(usage, status, on="lead_uuid_c", how="left")
    # Export
    writeToCSVToS3(usage, "usage")
    return usage

def createActivityData():
    # Perform ingestions
    groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets = ingestData()
    # Clean
    account = cleanAccount(account)
    salesforcecampaign = cleanCampaign(salesforcecampaign)
    # Merge lead and contact
    lead = lead[["lead_id", "lead_lean_data_reporting_matched_account_c", "lead_uuid_c"]]
    contact = contact[["ctct_id", "ctct_account_id", "ctct_uuid_c"]]
    contact.columns = ["lead_id", "lead_lean_data_reporting_matched_account_c", "lead_uuid_c"]
    lead = pd.concat([lead, contact], axis=0)
    # Merge campaign and campaignmember data
    new = pd.merge(salesforcecampaign, salesforcecampaignmember, left_on="sfc_name", right_on="sfcm_campaign_name_text", how="inner")
    # Merge lead/contact with campaignmember
    new = pd.merge(new, lead, left_on="sfcm_lead_or_contact_id", right_on="lead_id", how="inner")
    # Merge with account
    new = pd.merge(account, new, left_on="acc_account_id_long_version__c", right_on="lead_lean_data_reporting_matched_account_c", how="right")
    # Merge with dgc users
    completeLeft = pd.merge(new, dgc_users, left_on="lead_uuid_c", right_on="usr_id", how="inner")
    # Merge with activity data
    activity = pd.merge(completeLeft, pendo_activity, left_on="usr_id", right_on="pa_visitorid", how="left")
    # Add status
    status = addPersonalisedStatus(activity)
    activity = pd.merge(activity, status, on="lead_uuid_c", how="left")
    # Export
    writeToCSVToS3(activity, "activity")
    return completeLeft, status, pendo_usage, assets

def addPersonalisedStatus(activity):
    new = activity[["usr_createdOn", "lead_uuid_c", "pa_numminutes"]]
    # Get total usage per user
    new["total_minutes"] = new.groupby(["lead_uuid_c"])["pa_numminutes"].transform(sum)
    # Clean
    new = new.drop_duplicates(subset=['lead_uuid_c'])
    # Create message
    now = datetime.now(pytz.timezone('utc'))
    testDriveStatus = []
    for index, row in new.iterrows():
        start = pd.to_datetime(row["usr_createdOn"], unit='ms', utc = True)
        if (row["total_minutes"] == 0) & ((now - start).days > 7):
            testDriveStatus.append("Wasted test-drive (not logged in after a week)")
        elif ((row["total_minutes"] > 0)) & ((now - start).days >= 7):
            testDriveStatus.append("Test-drive period is over and was used")
        elif ((row["total_minutes"] > 0)) & ((now - start).days < 7):
            testDriveStatus.append("Test-drive in progress")
        elif (not(row["total_minutes"] > 0)) & ((now - start).days >= 3):
            testDriveStatus.append("At very high risk (not logged in after 72 hours)")
        elif (not(row["total_minutes"] > 0)) & ((now - start).days >= 2):
            testDriveStatus.append("At high risk (not logged in after 48 hours)")
        elif (not(row["total_minutes"] > 0)) & ((now - start).days >= 1):
            testDriveStatus.append("At risk (not logged in after 24 hours)")
        elif (not(row["total_minutes"] > 0)) & ((now - start).days == 0):
            testDriveStatus.append("Registered less than 24 hours ago and no usage")
        else:
            testDriveStatus.append("Problem")
    # Clean
    testDriveStatus = pd.DataFrame(testDriveStatus)
    testDriveStatus.columns = ["act_status"]
    new = new.reset_index(drop = True)
    testDriveStatus = testDriveStatus.reset_index(drop = True)
    status = pd.concat([new, testDriveStatus], axis = 1)
    status = status[["lead_uuid_c", "act_status"]]
    return status

def ingestAllCampaigns():
    # Engine
    importer = redshiftExporter("testdriveanalysis", "", False)
    con = importer.import_data()
    con.set_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT")
    # Ingest campaign data
    sql = "select * from salesforce.campaign"
    salesforcecampaign = pd.read_sql_query(sql, con)
    # Ingest campaign member data
    sql = "select * from salesforce.campaign_member"
    salesforcecampaignmember = pd.read_sql_query(sql, con)
    # Merge
    all_campaigns = pd.merge(salesforcecampaign, salesforcecampaignmember, left_on="name", right_on="campaign_name_text", how="left")
    # Clean
    all_campaigns = all_campaigns[["campaign_id", "campaign_name_text", "first_responded_date", "start_date", "email"]]
    # Write to S3
    writeToCSVToS3(all_campaigns, "all_campaigns")
    # Close connection
    con.close()

def ingestUniversity():
    # Engine
    importer = redshiftExporter("testdriveanalysis", "", False)
    con = importer.import_data()
    con.set_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT")
    # Ingest campaign data
    sql = "select name, status, username from university.enrollments"
    enrollments = pd.read_sql_query(sql, con)
    # Write to S3
    writeToCSVToS3(enrollments, "enrollments")
    # Close connection
    con.close()

def enhanceReadability(groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets):
    groups = groups.add_prefix('gr_')
    dgc_users = dgc_users.add_prefix('usr_')
    pendo_activity = pendo_activity.add_prefix('pa_')
    pendo_usage = pendo_usage.add_prefix('pu_')
    salesforcecampaign = salesforcecampaign.add_prefix('sfc_')
    salesforcecampaignmember = salesforcecampaignmember.add_prefix('sfcm_')
    lead = lead.add_prefix('lead_')
    contact = contact.add_prefix('ctct_')
    account = account.add_prefix('acc_')
    assets = assets.add_prefix('as_')
    return groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets

def ingestData():
    pendo_activity, pendo_usage = ingestPendo()
    salesforcecampaign, salesforcecampaignmember, lead, contact, account = ingestSalesforce()
    groups = readCSVFromS3("groups")
    dgc_users = readCSVFromS3("dgc_users")
    assets = readCSVFromS3("asset")
    # Enhance readability
    groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets = enhanceReadability(groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets)
    return groups, dgc_users, pendo_activity, pendo_usage, salesforcecampaign, salesforcecampaignmember, lead, contact, account, assets

def ingestSalesforce():
    # Engine
    importer = redshiftExporter("testdriveanalysis", "", False)
    con = importer.import_data()
    con.set_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT")
    # Ingest campaign data
    sql = "select * from salesforce.campaign where name = 'GBL-21-Q1-TDR-Test-Drive'"
    salesforcecampaign = pd.read_sql_query(sql, con)
    # Ingest campaign member data
    sql = "select * from salesforce.campaign_member where campaign_name_text = 'GBL-21-Q1-TDR-Test-Drive'"
    salesforcecampaignmember = pd.read_sql_query(sql, con)
    # Ingest lead data
    sql = "select * from salesforce.lead"
    lead = pd.read_sql_query(sql, con)
    # Ingest contact data
    sql = "select * from salesforce.contact"
    contact = pd.read_sql_query(sql, con)
    # Ingest account data
    sql = "select * from salesforce.account"
    account = pd.read_sql_query(sql, con)
    # Close connection
    con.close()
    return salesforcecampaign, salesforcecampaignmember, lead, contact, account

def ingestPendo():
    INSTANCE_NAME = "test-drive.collibra.com"
    # Engine
    importer = redshiftExporter("testdriveanalysis", "", False)
    con = importer.import_data()
    con.set_isolation_level("ISOLATION_LEVEL_AUTOCOMMIT")
    # Ingest pendo activity
    sql = "select * from productusage.pendo_activity where instanceid LIKE '%" + INSTANCE_NAME + "%' and date >= '2021-04-12'"
    pendo_activity = pd.read_sql_query(sql, con)
    # Ingest pendo usage
    sql = "select * from productusage.pendo_usage where instanceid LIKE '%" + INSTANCE_NAME + "%' and date >= '2021-04-12'"
    pendo_usage = pd.read_sql_query(sql, con)
    # Close connection
    con.close()
    return pendo_activity, pendo_usage

def writeToCSVToS3(dataset, description):
    env = os.environ["environment"]
    csv_buffer = StringIO()
    dataset.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(get_bucket(env), f"clean/testdriveanalysis/{description}.csv").put(Body=csv_buffer.getvalue())

def readCSVFromS3(description):
    env = os.environ["environment"]
    bucket = get_bucket(env)
    key = f"raw/testdriveanalysis/{description}.csv"
    s3_resource = boto3.client('s3')
    obj = s3_resource.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df

def get_bucket(env: str):
    return 'cdo-datalake-prd' if env == 'prd' else 'cdo-datalake-dev-bphcob'

def cleanAccount(account):
    # Selection of all columns with a null value, fixed, to be reviewed,
    toBeDropped = [
     'acc___index_level_0__',
     'acc_bi_business_objects__c',
     'acc_product_connect__c',
     'acc_rkpi2_deleted_from_rain_king__c',
     'acc_use_case_big_data_analytics__c',
     'acc_product_helpdesk__c',
     'acc_run_as_current_user__c',
     'acc_product_gdpr_accelerator__c',
     'acc_account_counter__c',
     'acc_product_professional_services__c',
     'acc_use_case_data_catalog_dictionary__c',
     'acc_fferpcore_is_billing_address_validated__c',
     'acc_account_owner_active__c',
     'acc_netsuite_conn_push_to_net_suite__c',
     'acc_use_case_business_glossary__c',
     'acc_activity_total_scored__c',
     'acc_scoring_matrix_model_1__c',
     'acc_product_coaching_services__c',
     'acc_has_open_opps__c',
     'acc_one_time_trigger_12_11__c',
     'acc_product_reference_data__c',
     'acc_use_case_data_quality__c',
     'acc_bi_tableau__c',
     'acc_bi_oracle_obiee__c',
     'acc_data_citizens_2016__c',
     'acc_data_governance__c',
     'acc_at_risk__c',
     'acc_data_citizens_2017__c',
     'acc_test_program_master_agreement__c',
     'acc_product_dictionary__c',
     'acc_netsuite_conn_celigo_update__c',
     'acc_annual_revenue_scored__c',
     'acc_product_policy_manager__c',
     'acc_connect_purchased__c',
     'acc_use_case_issue_management__c',
     'acc_bi_sharepoint__c',
     'acc_last_activity_scored__c',
     'acc_number_of_activities_leads__c',
     'acc_valid_evaluation_licenses__c',
     'acc_amount_of_opps__c',
     'acc_bi_cognos__c',
     'acc_use_case_reference_data__c',
     'acc_netsuite_conn_sync_in_progress__c',
     'acc_product_author_users__c',
     'acc_use_case_regulatory_reporting__c',
     'acc_business_unit_scoring__c',
     'acc_power_of_one__c',
     'acc_industry_scored__c',
     'acc_number_of_open_opps__c',
     'acc_employees_scored__c',
     'acc_ft500__c',
     'acc_go_live_award__c',
     'acc_product_concurrent_users__c',
     'acc_netsuite_conn_pushed_from_opportunity__c',
     'acc_of_new_business_opps__c',
     'acc_product_catalog__c',
     'acc_number_of_activities_total__c',
     'acc_cloud__c',
     'acc_referenceable__c',
     'acc_use_case_data_sharing_agreements__c',
     'acc_scoring_matrix_model_2__c',
     'acc_bi_spotfire__c',
     'acc_bi_microsoft_bi__c',
     'acc_use_case_report_certification__c',
     'acc_risk_audit_compliance_regulatory__c',
     'acc_data_management__c',
     'acc_bi_qlikview__c',
     'acc_analytics__c',
     'acc_my_team__c',
     'acc_number_of_open_renewals__c',
     'acc_use_case_financial_compliance_bcbs_239__c',
     'acc_fferpcore_is_shipping_address_validated__c',
     'acc_use_case_critical_data_elements__c',
     'acc_inside_view_sic_description__c',
     'acc_upp__c',
     'acc_customer_recruitment_notes__c',
     'acc_customer_health_index__c',
     'acc_description',
     'acc_number_of_support_contacts__c',
     'acc_inside_view_sic__c',
     'acc_integration_points__c',
     'acc_use_case_other_use_case__c',
     'acc_last_modified_by_id',
     'acc_connect_informatica_dq_idq__c',
     'acc_fferpcore_validated_shipping_city__c',
     'acc_fferpcore_validated_shipping_country__c',
     'acc_aba_routing__c',
     'acc_team__c',
     'acc_connect_informatica_mdm__c',
     'acc_connect_ab_initio__c',
     'acc_d_b_industry_description__c',
     'acc_qbdialer_last_call_time__c',
     'acc_connect_in_production__c',
     'acc_logo_usage_external__c',
     'acc_shipping_postal_code',
     'acc_best_eloqua_score__c',
     'acc_project_manager__c',
     'acc_price_increase__c',
     'acc_pendo_events__c',
     'acc_community_customer_story__c',
     'acc_auto_renewal__c',
     'acc_primary_country__c',
     'acc_customer_termination_reason_long_text__c',
     'acc_billing_geocode_accuracy',
     'acc_customer_success_manager__c',
     'acc_engagio_status__c',
     'acc_rrpu_alert_message__c',
     'acc_zendesk_account_manager__c',
     'acc_analyst_report_participation__c',
     'acc_phone',
     'acc_currency_iso_code',
     'acc_integration_engineer__c',
     'acc_system_modstamp',
     'acc_renewal_price_increase__c',
     'acc_connection_received_id',
     'acc_fferpcore_validated_billing_country__c',
     'acc_meetups_leader__c',
     'acc_customer_start_quarter__c',
     'acc_x5_1_migration_comment__c',
     'acc_open_report_in_tableau__c',
     'acc_industry_group__c',
     'acc_unengaged_partners__c',
     'acc_pendo_last_visit__c',
     'acc_in_production__c',
     'acc_case_study_created__c',
     'acc_supporting_sales_rep__c',
     'acc_nip_band__c',
     'acc_test_field__c',
     'acc_lifecycle__c',
     'acc_secondary_at_risk_reason__c',
     'acc_rkpi2_rk_retrieval_flag__c',
     'acc_jigsaw_company_id',
     'acc_requested_account_owner__c',
     'acc_photo_url',
     'acc_shipping_city',
     'acc_no_of_employees__c',
     'acc_reason_for_production_delay__c',
     'acc_collibra_executive_sponsor__c',
     'acc_bizible2_engagement_score__c',
     'acc_latest_product_version__c',
     'acc_roadmap_qbr_positioned__c',
     'acc_billing_state_code',
     'acc_master_record_id',
     'acc_champion__c',
     'acc_case_study_file_attached_to_account__c',
     'acc_name_usage_external__c',
     'acc_primary_state__c',
     'acc_customer_health_index_reason__c',
     'acc_fferpcore_validated_shipping_postal_code__c',
     'acc_customer_story__c',
     'acc_created_by_id',
     'acc_status_protect_expand__c',
     'acc_shipping_geocode_accuracy',
     'acc_account_annual_revenue__c',
     'acc_name_usage_internal__c',
     'acc_pendo_time_on_site_in_minutes__c',
     'acc_fax',
     'acc_id',
     'acc_shipping_street',
     'acc_executive_cadence_meetings__c',
     'acc_customer_termination_reason__c',
     'acc_inside_view_naics__c',
     'acc_customer_end_quarter__c',
     'acc_inside_view_industry__c',
     'acc_rkpi2_rk_default_visibility__c',
     'acc_deployment_type__c',
     'acc_outreach_september_candidates__c',
     'acc_test__c',
     'acc_nip_health_index__c',
     'acc_name',
     'acc_account_created_by_role__c',
     'acc_miscellaneous_engagements__c',
     'acc_date_of_first_purchase__c',
     'acc_bi_other__c',
     'acc_rk_state__c',
     'acc_x5_1_migration_status__c',
     'acc_inside_view__c',
     'acc_primary_at_risk_reason__c',
     'acc_partner_stage__c',
     'acc_fferpcore_validated_billing_city__c',
     'acc_connect_ibm_reference_data_management__c',
     'acc_discover_org_industry__c',
     'acc_shipping_country_code',
     'acc_netsuite_conn_credit_hold__c',
     'acc_connect_ibm_business_glossary__c',
     'acc_billing_city',
     'acc_fferpcore_validated_shipping_street__c',
     'acc_attributes_url',
     'acc_account_owner_role__c',
     'acc_data_privacy_addendum__c',
     'acc_customer_start_year__c',
     'acc_i_sell_os_key_id__c',
     'acc_sdr_name__c',
     'acc_connect_global_i_ds__c',
     'acc_journey_stage__c',
     'acc_qbdialer_time_zone_sid_key__c',
     'acc_d_b_duns_number__c',
     'acc_record_type_id',
     'acc_fferpcore_validated_billing_street__c',
     'acc_collibra_customer_success_manager_name__c',
     'acc_sub_industry__c',
     'acc_d_b_employees__c',
     'acc_target_account__c',
     'acc_use_case__c',
     'acc_legal_name__c',
     'acc_pendo_usage_trending__c',
     'acc_termination_for_convenience__c',
     'acc_d_b_industry_code__c',
     'acc_date_partition',
     'acc_customer_executive_sponsor__c',
     'acc_at_risk_notes__c',
     'acc_cam__c',
     'acc_connect_informatica_imm__c',
     'acc_speaking_engagement__c',
     'acc_connect_service_now__c',
     'acc_connect_other__c',
     'acc_enablement_status__c',
     'acc_fferpcore_validated_shipping_state__c',
     'acc_revenue_segment__c',
     'acc_liability_cap__c',
     'acc_shipping_state',
     'acc_exception_reason__c',
     'acc_go_live_award_purchase_order__c',
     'acc_type_and_target__c',
     'acc_current_instance__c',
     'acc_customer_start_fy_quarter__c',
     'acc_msa_entity__c',
     'acc_netsuite_conn_channel_tier__c',
     'acc_parent_id',
     'acc_inside_view_naics_description__c',
     'acc_netsuite_conn_net_suite_id__c',
     'acc_shipping_country',
     'acc_requested_at_risk_status__c',
     'acc_connect_ibm_information_analyzer__c',
     'acc_termination_for_convenience_notes__c',
     'acc_jigsaw',
     'acc_speaking_engagement_notes__c',
     'acc_previous_journey_stage__c',
     'acc_go_live_award_recipient__c',
     'acc_cam_notes__c',
     'acc_connect_oracle_drm__c',
     'acc_connect_use_cases__c',
     'acc_permission_to_use_logo_name__c',
     'acc_market__c',
     'acc_fferpcore_validated_billing_postal_code__c',
     'acc_billing_contact__c',
     'acc_customer_story_notes__c',
     'acc_owner_id',
     'acc_requested_account_owner_lookup__c',
     'acc_dgc_platform__c',
     'acc_fferpcore_validated_billing_state__c',
     'acc_current_state__c',
     'acc_certified_rangers__c',
     'acc_renewal_quarter__c',
     'acc_go_live_award_delivery_name_and_address__c',
     'acc_connect_informatica_rdm__c',
     'acc_shipping_state_code',
     'acc_junior_ae__c',
     'acc_attributes_type',
     'acc_license_type__c',
     'acc_billing_state',
     'acc_pendo_days_active__c',
     'acc_at_risk_status__c',
     'acc_netsuite_conn_net_suite_sync_err__c',
     'acc_account_transfer_notes__c',
     'acc_seller__c',
     'acc_fferpcore_exemption_certificate__c',
     'acc_pendo_visitors__c',
     'acc_created_by_role__c',
     'acc_account_owner_s_manager__c',
     'acc_product_expert__c',
     'acc_connection_sent_id',
     'acc_sdr__c',
     'acc_connect_oracle_edq__c',
     'acc_awards__c',
     'acc_team_static__c',
     'acc_dozisf_zoom_info_first_updated__c',
     'acc_lean_data_tag__c',
     'acc_partner_account__c',
     'acc_lean_data_reporting_customer__c',
     'acc_hot__c',
     'acc_at_risk_next_steps__c',
     'acc_advocate_hub_referrer_account__c',
     'acc_fferpcore_tax_code1__c',
     'acc_reference_program_participant__c',
     'acc_fferpcore_sales_tax_status__c',
     'acc_fferpcore_tax_code3__c',
     'acc_lean_data_reporting_target_account__c',
     'acc_advocate_hub_referral_source__c',
     'acc_ultimate_parent_account_id__c',
     'acc_del_us_region_1__c',
     'acc_fferpcore_tax_code2__c',
     'acc_territory_disrupted__c',
     'acc_connect_use_cases_other__c',
     'acc_target_account_emea_2020__c',
     'acc_fferpcore_vat_status__c',
     'acc_territory_owner_update__c',
     'acc_foreign_government__c',
     'acc_is_customer_portal',
     'acc_potential_target_account__c',
     'acc_outreach_cadence__c',
     'acc_stp_migration_lead__c',
     'acc_ecustoms_im_status__c',
     'acc_cam_migration_notes__c',
     'acc_fferpcore_output_vat_code__c',
     'acc_c_data_drivers_currently_in_trial__c',
     'acc_migration_to_cloud_stage__c',
     'acc_has_known_advocates__c',
     'acc_fferpcore_vat_registration_number__c',
     'acc_customer_success_role__c',
     'acc_abm_account__c',
     'acc_ecustoms_screening_trigger__c',
     'acc_tax_exempt__c',
     'acc_renewal_notice__c',
     'acc_resource_assignment__c',
     'acc_lean_data_reporting_has_opportunity__c',
     'acc_fferpcore_tax_country_code__c',
     'acc_set_custom_split__c',
     'acc_lean_data_ld_email_domain__c',
     'acc_lean_data_ld_email_domains__c',
     'acc_lean_data_scenario_2_owner__c',
     'acc_ffbf_payment_priority__c',
     'acc_authorized_services_subcontractor__c',
     'acc_advocate_hub_referral_id__c',
     'acc_lean_data_search__c',
     'acc_cam_manager__c',
     'acc_fferpcore_materialized_shipping_address_validated__c',
     'acc_lean_data_sla__c',
     'acc_stp_estimated_migration_quarter__c',
     'acc_ffbf_bank_bic__c',
     'acc_ffbf_payment_routing_method__c',
     'acc_qbdialer_related_contact_last_call_time__c',
     'acc_active_vs_original_arr_discrep__c',
     'acc_stp_owner__c',
     'acc_customer_account__c',
     'acc_big_bet_account_2020__c',
     'acc_state__c',
     'acc_lean_data_scenario_1_owner__c',
     'acc_lid_linked_in_company_id__c',
     'acc_tableau_instance_name__c',
     'acc_msa_id__c',
     'acc_lean_data_scenario_3_owner__c',
     'acc_renewal_manager__c',
     'acc_advocate_hub_referrer_company__c',
     'acc_collibra_executive_sponsor_secondary__c',
     'acc_dozisf_zoom_info_id__c',
     'acc_partner_capabilities__c',
     'acc_xdel_perpetual__c',
     'acc_ffbf_account_particulars__c',
     'acc_advocate_hub_referrer_name__c',
     'acc_opportunity_40__c',
     'acc_transfer_approval_status__c',
     'acc_open_renewal_original_arr__c',
     'acc_inside_view_sub_industries__c',
     'acc_advocate_hub_referrer_contact__c',
     'acc_dozisf_zoom_info_last_updated__c',
     'acc_sic_desc',
     'acc_ffbf_payment_code__c',
     'acc_ecustoms_rps_status__c',
     'acc_lean_data_scenario_4_owner__c',
     'acc_fferpcore_materialized_billing_address_validated__c',
     'acc_account_plan_review_status__c',
     'acc_matched_status__c',
     'acc_account_plan_link__c',
     'acc_lean_data_reporting_target_account_number__c',
     'acc_advocate_hub_referrer_email__c',
     'acc_sic',
     'acc_ffbf_payment_country_iso__c',
     'acc_payment_terms__c',
     'acc_authorized_reseller__c',
     'acc_collibra_csm__c',
     'acc_customer_sales_rep__c',
     'acc_known_company_policies__c',
     'acc_education_credits_used__c',
     'acc_education_credits_purchased__c',
     'acc_zendesk_organization_name__c',
     'acc_partner_tier__c',
     'acc_stp_migration_story__c',
     'acc_gcp_contact__c',
     'acc_aws_contact__c',
     'acc_zoom_info_postal_code__c',
     'acc_zoom_info_street__c',
     'acc_zoom_info_country__c',
     'acc_zoom_info_industry__c',
     'acc_zoom_info_state__c',
     'acc_zoom_info_city__c',
     'acc_sbqq_co_termination_event__c',
     'acc_sbqq_preserve_bundle__c',
     'acc_sbqq_tax_exempt__c',
     'acc_sbqq_renewal_model__c',
     'acc_sbqq_renewal_pricing_method__c',
     'acc_sbqq_default_opportunity__c',
     'acc_sbqq_co_termed_contracts_combined__c',
     'acc_sbqq_contract_co_termination__c',
     'acc_sbqq_asset_quantities_combined__c',
     'acc_sbqq_ignore_parent_contracted_prices__c',
     'acc_sbqq_price_hold_end__c',
     'acc_tax_exempt_letter_attached__c',
     'acc_ultimate__c',
     'acc_territory__c',
     'acc_ultimate_parent_account__c',
     'acc_abn_vat_number__c',
     'acc_partner_contract_name__c',
     'acc_strategic_partner__c',
     'acc_customer_summary__c',
     'acc_presales_manager__c',
     'acc_zoom_info_employees__c',
     'acc_zoom_info_description__c',
     'acc_account_natural_name__c',
     'acc_customer_documents__c',
     'acc_has_integrator_products__c',
     'acc_website']
    account = account.drop(toBeDropped, axis=1, errors= "ignore")
    return account

def cleanCampaign(salesforcecampaign):
    # Campaign is only filtered on test-drive, so this info is not useful
    salesforcecampaign = salesforcecampaign[["sfc_created_date", "sfc_id", "sfc_name", "sfc_start_date", "sfc_type"]]
    return salesforcecampaign
