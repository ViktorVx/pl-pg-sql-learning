DO $$
    BEGIN
        CREATE TYPE pg_temp.log_row AS (process_id varchar(36), process_name VARCHAR(128), log_level_value VARCHAR(32), message_value varchar(512));
    EXCEPTION
        WHEN duplicate_object THEN null;
    END;
    $$;

CREATE OR REPLACE FUNCTION pg_temp.getNewOperationName(oper_name_val VARCHAR, oper_id_val VARCHAR) RETURNS VARCHAR LANGUAGE plpgsql AS $$
    DECLARE
        new_oper_name     ${schema}.UN_OPER.OPERATION_NAME%TYPE;
        oper_count NUMERIC;
    BEGIN
        SELECT count(*) INTO oper_count FROM ${schema}.OPER op WHERE op.NAME = oper_name_val;
        IF oper_count = 1 THEN
            new_oper_name := oper_name_val;
        ELSE
            new_oper_name := oper_name_val || '_' || oper_id_val;
        END IF;
        RETURN new_oper_name;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.migrateOperations(proc_uuid VARCHAR, INOUT log_array pg_temp.log_row[]) LANGUAGE plpgsql AS $$
    DECLARE
        curs1 REFCURSOR;
        operation_record ${schema}.OPER%ROWTYPE;
        new_oper_name    ${schema}.UN_OPER.OPERATION_NAME%TYPE;
        exists_uo        NUMERIC := 0;
    BEGIN
        OPEN curs1 FOR SELECT * FROM ${schema}.OPER;
        LOOP
            FETCH curs1 INTO operation_record;
            EXIT WHEN NOT FOUND;
            CALL pg_temp.logMessage('INFO', FORMAT('Migrate operation with id: %s', operation_record.ENTITY_ID), proc_uuid, log_array);
            new_oper_name := pg_temp.getNewOperationName(operation_record.NAME, operation_record.ENTITY_ID);

            select count(*) into exists_uo from ${schema}.UN_OPER uo where uo.operation_name = new_oper_name;
            IF exists_uo = 0 THEN
                INSERT INTO ${schema}.UN_OPER(OPERATION_NAME, CHANNEL_NAME, OPERATION_PERMISSION, OPERATION_SUBSYSTEM,
                                                OPERATION_TITLE, SUB_SYSTEM_TYPE, SHORT_TITLE, AVAILABLE_IN_SI)
                values (new_oper_name,
                        operation_record.CHANNEL_NAME,
                        operation_record.PERMISSION,
                        operation_record.SUBSYSTEM,
                        operation_record.TITLE,
                        operation_record.SUB_SYSTEM_TYPE,
                        operation_record.SHORT_TITLE,
                        operation_record.IS_AVAILABLE_IN_STANDIN);
                INSERT INTO ${schema}.OPER_VERSION(VERSION_ID, OPERATION_NAME, CATALOG_ID, VERSION_VALUE, RESOURCE_URL,
                                                 WEIGHT, BACKGROUND, KEY_WORDS, KEY_WORD_SYNONYMS, BLOCKED_FLAG,
                                                 BLOCKED_MESSAGE, PILOT_FLAG, SUP_PARAM_ID, MODIFIED_DATE, ROW_VERSION,
                                                 LOCKED_FLAG, NEED_REPLICATION)
                VALUES (UPPER(MD5(new_oper_name || '|01.00')),
                        new_oper_name, operation_record.CATALOG_ID, '01.00', operation_record.URL,
                        operation_record.WEIGHT,
                        operation_record.BACKGROUND, operation_record.KEY_WORDS, operation_record.KEY_WORD_SYNONYMS,
                        operation_record.IS_BLOCKED, operation_record.BLOCKED_MESSAGE, operation_record.IS_PILOT_ZONE,
                        operation_record.SUP_PARAM_ID, operation_record.MODIFIED_DATE, operation_record.ROW_VERSION,
                        operation_record.LOCKED_FLAG, operation_record.NEED_REPLICATION);
            END IF;
        END LOOP;
        CLOSE curs1;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.migratePilotZones(proc_uuid VARCHAR, INOUT log_array pg_temp.log_row[]) LANGUAGE plpgsql AS $$
    DECLARE
        pilot_zone_record ${schema}.P_ZN%ROWTYPE;
        operation_record  ${schema}.OPER%ROWTYPE;
        oper_inner_record  ${schema}.OPER%ROWTYPE;
        version_record ${schema}.OPER_VERSION%ROWTYPE;
        tenant_record ${schema}.TEN%ROWTYPE;
        group_code_full   VARCHAR(1000);
        group_code_val    ${schema}.P_GR.GROUP_CODE%TYPE;
        group_title_val   ${schema}.P_GR.GROUP_TITLE%TYPE;
        new_oper_name     ${schema}.UN_OPER.OPERATION_NAME%TYPE;
        new_oper_inner_name  ${schema}.UN_OPER.OPERATION_NAME%TYPE;
        exists_pilot_group NUMERIC := 0;
        curs1 REFCURSOR;
        curs2 REFCURSOR;
        curs3 REFCURSOR;
    BEGIN
        OPEN curs1 FOR SELECT * FROM ${schema}.P_ZN;
        LOOP
            FETCH curs1 INTO pilot_zone_record;
            EXIT WHEN NOT FOUND;
            CALL pg_temp.logMessage('INFO', FORMAT('Migrate pilot zone with id: %s', pilot_zone_record.ENTITY_ID), proc_uuid, log_array);
            SELECT * INTO operation_record FROM ${schema}.OPER op WHERE op.ENTITY_ID = pilot_zone_record.REFERENCE_ID;
            select pg_temp.getNewOperationName(operation_record.name, oper_inner_record.entity_id)
                into new_oper_name;

            OPEN curs2 FOR
                SELECT DISTINCT ST.* FROM ${schema}.EN_TEN ET
                                              LEFT JOIN ${schema}.TEN ST ON ET.TENANT_ID = ST.ID
                WHERE ET.ENTITY_ID = operation_record.ENTITY_ID;
            LOOP
                FETCH curs2 INTO tenant_record;
                EXIT WHEN NOT FOUND;
                group_code_full := pilot_zone_record.tb || '_' ||
                                   CASE
                                       WHEN pilot_zone_record.GOSB IS NULL THEN 'NULL'
                                       WHEN pilot_zone_record.GOSB = '' THEN 'NULL'
                                       ELSE pilot_zone_record.GOSB
                                       END || '_' ||
                                   CASE
                                       WHEN pilot_zone_record.vsp IS NULL THEN 'NULL'
                                       WHEN pilot_zone_record.vsp = '' THEN 'NULL'
                                       ELSE pilot_zone_record.vsp
                                       END || '_' || tenant_record.tenant_code;
                group_code_val  := substring(group_code_full, 1, 64);
                group_title_val := substring(group_code_full, 1, 128);
                SELECT count(*) INTO exists_pilot_group FROM ${schema}.P_GR pg where pg.GROUP_CODE = group_code_val;
                IF exists_pilot_group = 0 THEN
                    -- Создаем пилотную группу
                    INSERT INTO ${schema}.P_GR(GROUP_CODE, GROUP_TITLE) VALUES (group_code_val, group_title_val);
                    -- Добавляем привязку к тенанту для пилотной группы
                    INSERT INTO ${schema}.P_GR_TENANT(GROUP_TENANT_ID, TENANT_ID, GROUP_CODE)
                    VALUES (
                               UPPER(MD5(tenant_record.ID || '|' || group_code_val)),
                               tenant_record.ID,
                               group_code_val
                           );
                    -- Добавляем участников группы пилотирования
                    INSERT INTO ${schema}.P_GR_MEMBER (MEMBER_ID, GROUP_CODE, TB, GOSB, VSP)
                    VALUES (
                               UPPER(MD5(group_code_val || '|' ||
                                         pilot_zone_record.TB || '|' ||
                                         CASE
                                             WHEN pilot_zone_record.GOSB IS NULL THEN 'null'
                                             WHEN pilot_zone_record.GOSB = '' THEN 'null'
                                             ELSE pilot_zone_record.GOSB
                                             END || '|' ||
                                         CASE
                                             WHEN pilot_zone_record.vsp IS NULL THEN 'null'
                                             WHEN pilot_zone_record.vsp = '' THEN 'null'
                                             ELSE pilot_zone_record.vsp
                                             END || '|' || 'null')
                                   ),
                               group_code_val,
                               pilot_zone_record.TB,
                               pilot_zone_record.GOSB,
                               pilot_zone_record.VSP);
                    -- Добавляем версию соответствующую операции старой пилотной зоны
                    SELECT * INTO version_record FROM ${schema}.OPER_VERSION ov
                    WHERE ov.OPERATION_NAME = new_oper_name AND ov.VERSION_VALUE = '01.00';
                    INSERT INTO ${schema}.P_GR_OPERATION (OPER_GROUP_ID, OPER_VERS_ID, GROUP_CODE)
                    VALUES (UPPER(MD5(version_record.VERSION_ID || '|' || group_code_val)),
                            version_record.VERSION_ID,
                            group_code_val) ON CONFLICT DO NOTHING;
                    -- Добавляем все версии без флага пилотирования и, содержащие тенант пилотной группы
                    OPEN curs3 FOR
                        SELECT SOV.* FROM ${schema}.OPER_VERSION SOV
                                              LEFT JOIN ${schema}.UN_OPER SUO on SOV.OPERATION_NAME = SUO.OPERATION_NAME
                                              LEFT JOIN ${schema}.UN_OPER_TENANT SUOT on SUO.OPERATION_NAME = SUOT.OPERATION_NAME
                        WHERE SOV.PILOT_FLAG = 'N' AND SUOT.TENANT_ID = tenant_record.id;
                    LOOP
                        FETCH curs3 INTO version_record;
                        EXIT WHEN NOT FOUND;
                        INSERT INTO ${schema}.P_GR_OPERATION (OPER_GROUP_ID, OPER_VERS_ID, GROUP_CODE)
                        VALUES (UPPER(MD5(version_record.VERSION_ID || '|' || group_code_val)),
                                version_record.VERSION_ID,
                                group_code_val) ON CONFLICT DO NOTHING;
                    END LOOP;
                    CLOSE curs3;
                    -- Добавляем все версии с флагом пилотной зоны и, содержащие тенант пилотной группы и
                    -- TB+GOSB+VSP как в пилотной группе
                    OPEN curs3 FOR
                        SELECT SO.* FROM ${schema}.P_ZN pz
                                             LEFT JOIN ${schema}.OPER SO ON pz.REFERENCE_ID = SO.ENTITY_ID
                                             LEFT JOIN ${schema}.EN_TEN ET ON SO.ENTITY_ID = ET.ENTITY_ID
                        WHERE pz.TB = pilot_zone_record.TB AND
                            (coalesce(pz.GOSB, 'NULL') = coalesce(pilot_zone_record.GOSB, 'NULL')) AND
                            (coalesce(pz.VSP, 'NULL') = coalesce(pilot_zone_record.VSP, 'NULL')) AND
                                pz.REFERENCE_ID <> pilot_zone_record.REFERENCE_ID AND
                                ET.TENANT_ID = tenant_record.ID;
                    LOOP
                        FETCH curs3 INTO oper_inner_record;
                        EXIT WHEN NOT FOUND;
                        select pg_temp.getNewOperationName(oper_inner_record.name, oper_inner_record.entity_id)
                        into new_oper_inner_name;

                        SELECT * INTO version_record FROM ${schema}.OPER_VERSION ov
                        WHERE ov.OPERATION_NAME = new_oper_inner_name AND ov.VERSION_VALUE = '01.00';
                        INSERT INTO ${schema}.P_GR_OPERATION (OPER_GROUP_ID, OPER_VERS_ID, GROUP_CODE)
                        VALUES (UPPER(MD5(version_record.VERSION_ID || '|' || group_code_val)),
                                version_record.VERSION_ID,
                                group_code_val) ON CONFLICT DO NOTHING;
                    END LOOP;
                    CLOSE curs3;
                END IF;
            END LOOP;
            CLOSE curs2;
        END LOOP;
        CLOSE curs1;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.migrateCatalogTenantLinks(proc_uuid VARCHAR, INOUT log_array pg_temp.log_row[]) LANGUAGE plpgsql AS $$
    DECLARE
        et_record   ${schema}.EN_TEN%ROWTYPE;
        curs1 REFCURSOR;
    BEGIN
        OPEN curs1 FOR SELECT * FROM ${schema}.EN_TEN et WHERE et.ENTITY_ID IN (SELECT cat.ENTITY_ID FROM ${schema}.CAT cat);
        LOOP
            FETCH curs1 INTO et_record;
            EXIT WHEN NOT FOUND;
            CALL pg_temp.logMessage('INFO', FORMAT('Migrate catalog tenant link, catalog id: %s, tenant_id: %s',
                                                   et_record.ENTITY_ID, et_record.TENANT_ID), proc_uuid, log_array);

            INSERT INTO ${schema}.CAT_TEN(CATALOG_TENANT_ID, CATALOG_ID, TENANT_ID)
            VALUES (UPPER(MD5(et_record.ENTITY_ID || '|' || et_record.TENANT_ID)),
                    et_record.ENTITY_ID,
                    et_record.TENANT_ID) ON CONFLICT DO NOTHING;
        END LOOP;
        CLOSE curs1;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.migrateOperationTenantLinks(proc_uuid VARCHAR, INOUT log_array pg_temp.log_row[]) LANGUAGE plpgsql AS $$
    DECLARE
        et_record        ${schema}.EN_TEN%ROWTYPE;
        new_oper_name    ${schema}.UN_OPER.OPERATION_NAME%TYPE;
        operation_record ${schema}.OPER%ROWTYPE;
        curs1 REFCURSOR;
    BEGIN
        OPEN curs1 FOR SELECT * FROM ${schema}.EN_TEN et WHERE et.ENTITY_ID IN (SELECT op.ENTITY_ID FROM ${schema}.OPER op);
        LOOP
            FETCH curs1 INTO et_record;
            EXIT WHEN NOT FOUND;
            CALL pg_temp.logMessage('INFO', FORMAT('Migrate operation tenant link, operation id: %s, tenant_id: %s',
                                                   et_record.ENTITY_ID, et_record.TENANT_ID), proc_uuid, log_array);
            ---------------------
            SELECT * INTO operation_record FROM ${schema}.OPER op WHERE op.ENTITY_ID = et_record.ENTITY_ID;
            new_oper_name := pg_temp.getNewOperationName(operation_record.NAME, operation_record.ENTITY_ID);
            ---------------------
            INSERT INTO ${schema}.UN_OPER_TENANT(OPERATION_TENANT_ID, OPERATION_NAME, TENANT_ID)
            VALUES (UPPER(MD5(new_oper_name || '|' || et_record.TENANT_ID)),
                    new_oper_name,
                    et_record.TENANT_ID) ON CONFLICT DO NOTHING;
        END LOOP;
        CLOSE curs1;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.logMessage(level_val VARCHAR, msg VARCHAR, process_id_val VARCHAR, INOUT log_array_val pg_temp.log_row[]) LANGUAGE plpgsql AS $$
    DECLARE
        process_name_val VARCHAR(256):= 'Data migration 20.1 -> 20.2';
    BEGIN
        select array_append(log_array_val, ROW(process_id_val, process_name_val, level_val, msg)::pg_temp.log_row) into log_array_val;
    END;
    $$;

CREATE OR REPLACE PROCEDURE pg_temp.persistLogs(log_array_val pg_temp.log_row[]) LANGUAGE plpgsql AS $$
DECLARE
    l pg_temp.log_row;
BEGIN
    FOREACH l in ARRAY log_array_val LOOP
        INSERT INTO ${schema}.log_table(process_id, process_name, log_level_value, message_value)
        select l.process_id, l.process_name, l.log_level_value, l.message_value;
    END LOOP;
END;
$$;

DO $$
    DECLARE
        united_operations_exists NUMERIC:=0;
        catalog_tenant_links_exists NUMERIC:=0;
        united_operations_tenant_links_exists NUMERIC:=0;
        pilot_groups_exists NUMERIC:=0;
        proc_uuid VARCHAR;
        log_array pg_temp.log_row[];
        l pg_temp.log_row;
    BEGIN
        select random()::text into proc_uuid;

        -- Проверяем, что миграция еще не выполнялась
        SELECT count(*) INTO united_operations_exists FROM ${schema}.UN_OPER;
        SELECT count(*) INTO catalog_tenant_links_exists FROM ${schema}.CAT_TEN;
        SELECT count(*) INTO united_operations_tenant_links_exists FROM ${schema}.UN_OPER_TENANT;
        SELECT count(*) INTO pilot_groups_exists FROM ${schema}.P_GR;
        IF (united_operations_exists = 0 AND united_operations_tenant_links_exists = 0 AND
            catalog_tenant_links_exists = 0 AND pilot_groups_exists = 0) THEN
            ------------------------------------------------------------------------------------------------------------
            CALL pg_temp.logMessage('INFO', 'Operations migration started', proc_uuid, log_array);
            CALL pg_temp.migrateOperations(proc_uuid, log_array);
            CALL pg_temp.logMessage('INFO', 'Operations migration complete', proc_uuid, log_array);
            ------------------------------------------------------------------------------------------------------------
            CALL pg_temp.logMessage('INFO', 'Catalog tenant links migration started', proc_uuid, log_array);
            CALL pg_temp.migrateCatalogTenantLinks(proc_uuid, log_array);
            CALL pg_temp.logMessage('INFO', 'Catalog tenant links migration complete', proc_uuid, log_array);
            ------------------------------------------------------------------------------------------------------------
            CALL pg_temp.logMessage('INFO', 'Operation tenant links migration started', proc_uuid, log_array);
            CALL pg_temp.migrateOperationTenantLinks(proc_uuid, log_array);
            CALL pg_temp.logMessage('INFO', 'Operation tenant links migration complete', proc_uuid, log_array);
            ------------------------------------------------------------------------------------------------------------
            CALL pg_temp.logMessage('INFO', 'PilotZones migration started', proc_uuid, log_array);
            CALL pg_temp.migratePilotZones(proc_uuid, log_array);
            CALL pg_temp.logMessage('INFO', 'PilotZones migration complete', proc_uuid, log_array);
            ------------------------------------------------------------------------------------------------------------
        ELSE
            CALL pg_temp.logMessage('WARN', 'Новые таблицы 20.2 уже заполнены данными. Миграция не осуществлена',
                proc_uuid, log_array);
        END IF;

        CALL pg_temp.persistLogs(log_array);

        EXCEPTION
            WHEN others THEN
                CALL pg_temp.logMessage('ERROR', SQLERRM, proc_uuid, log_array);
                BEGIN
                    CALL pg_temp.persistLogs(log_array);
                    COMMIT;
                END;
                RAISE;
    END;
    $$;
/