-- Test schema for writing tables into
CREATE SCHEMA IF NOT EXISTS test AUTHORIZATION postgres;

-- Marketing attribution data table
CREATE TABLE IF NOT EXISTS marketing_data (
    amount real,
    app_name varchar(100),
    app_version varchar(100),
    campaign varchar(100),
    city varchar(100),
    click_ip inet,
    click_utc_timestamp varchar(50),
    country varchar(10),
    currency varchar(10),
    event_name varchar(100),
    event_utc_timestamp varchar(100),
    install_utc_timestamp varchar(100),
    is_first_event integer,
    is_organic integer,
    is_reengagement integer,
    is_viewthrough integer,
    limit_ad_tracking integer,
    longname varchar(100),
    network varchar(100),
    partner_site varchar(100),
    platform varchar(50),
    sdid varchar(500),
    singular_click_id varchar(500),
    site varchar(100),
    tracker_name varchar(100),
    user_id varchar(500),
    match_type varchar(100),
    partner_campaign varchar(500),
    partner_sub_campaign varchar(100),
    tracker_campaign_id varchar(500),
    tracker_campaign_name varchar(500),
    tracker_creative_id varchar(100),
    tracker_sub_campaign_id varchar(100),
    utm_campaign varchar(500),
    utm_content varchar(100),
    utm_medium varchar(100),
    utm_source varchar(100),
    utm_term varchar(100),
    purchase_receipt_valid integer
);
