-- テーブルの作成
CREATE TABLE timezone_test (
    id SERIAL PRIMARY KEY,
    description TEXT,
    ts TIMESTAMP,
    tstz TIMESTAMPTZ
);

-- タイムゾーン設定の確認用関数
CREATE OR REPLACE FUNCTION show_timezone_settings() RETURNS TABLE (
    parameter_name TEXT,
    parameter_value TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 'current_setting(''timezone'')' AS parameter_name, current_setting('timezone') AS parameter_value
    UNION ALL
    SELECT 'current_setting(''TimeZone'')' AS parameter_name, current_setting('TimeZone') AS parameter_value
    UNION ALL
    SELECT 'SHOW timezone' AS parameter_name, pg_catalog.set_config('timezone', current_setting('timezone'), false) AS parameter_value
    UNION ALL
    SELECT 'now()' AS parameter_name, now()::TEXT AS parameter_value
    UNION ALL
    SELECT 'CURRENT_TIMESTAMP' AS parameter_name, CURRENT_TIMESTAMP::TEXT AS parameter_value
    UNION ALL
    SELECT 'CURRENT_TIMESTAMP::timestamp' AS parameter_name, CURRENT_TIMESTAMP::timestamp::TEXT AS parameter_value
    UNION ALL
    SELECT 'CURRENT_TIMESTAMP::timestamptz' AS parameter_name, CURRENT_TIMESTAMP::timestamptz::TEXT AS parameter_value;
END;
$$ LANGUAGE plpgsql; 