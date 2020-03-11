CREATE OR REPLACE PROCEDURE bulk_xml_upload (p_batch_no varchar2) as

var_ac_brn varchar2(3);
var_total_entries number;
var_total_value number;
var_batch_no char(4);
var_user_id varchar2(15) := 'SYSTEM';
l_orgin_brn varchar2(4) := '100';
var_knt number;

var_dat_process date;
var_txn_code varchar2(3);
var_current_cycle char(6);
var_current_period char(3);

var_value_date date;
var_narration varchar2(250);
var_month varchar2(3);
var_period char(4);

var_month_name varchar2(3) := 'DEC';

var_total_entries_cr number;
var_total_value_cr  number;

var_total_entries_dr number;
var_total_value_dr  number;

 var_file_name varchar2(25);
 var_reference_narration varchar2(50);

var_branch_code varchar2(3);
 var_ccy  varchar2(3);
var_account varchar2(20);

var_exch_rate varchar2(10);

var_lcy_equi_amount number;

cursor get_account_listing is

    select  account ,narration,crdr_ind,amount,value_dt,comm,code,ccy,rate
    from xml_tab  where  file_name = var_file_name;


begin

    update xml_tab_master set batch_start = sysdate
    where batch_no  = p_batch_no;
    commit;

    begin

        select  file_name,reference_narration
        into var_file_name,var_reference_narration
        from xml_tab_master where batch_no = p_batch_no;


        select today into var_dat_process from skyeubs.sttm_dates where branch_code = l_orgin_brn;

        var_value_date := var_dat_process;
        var_month := to_char(var_dat_process,'MON');
        var_period := to_char(var_dat_process,'YYYY');


        select count(*),sum(amount)
        into var_total_entries_cr, var_total_value_cr
        from xml_tab  where  file_name  = var_file_name and  crdr_ind = 'C';

        select count(*),sum(amount)
        into var_total_entries_dr, var_total_value_dr
        from xml_tab  where  file_name   = var_file_name and  crdr_ind = 'D';

        var_knt := 1;

        select trim(current_period),trim(current_cycle) into var_current_period,var_current_cycle
        from sttm_branch where branch_code = l_orgin_brn;


        insert into skyeubs.detb_upload_master (BRANCH_CODE,SOURCE_CODE,BATCH_NO,TOTAL_ENTRIES,UPLOADED_ENTRIES,BALANCING,BATCH_DESC,MIS_REQUIRED,AUTO_AUTH,
            GL_OFFSET_ENTRY_REQD,UDF_UPLOAD_REQD,OFFSET_GL,TXN_CODE,DR_ENT_TOTAL,CR_ENT_TOTAL,USER_ID,UPLOAD_STAT,JOBNO,SYSTEM_BATCH,POSITION_REQD,MAKER_ID,
            MAKER_DT_STAMP,CHECKER_ID,CHECKER_DT_STAMP,MOD_NO,AUTH_STAT,RECORD_STAT,ONCE_AUTH,UPLOAD_DATE,UPLOAD_FILE_NAME)
            VALUES(l_orgin_brn,'DE_UPLOAD',p_batch_no,null,var_total_entries,'N',var_reference_narration,'N','N','N','N',NULL,NULL,var_total_entries_cr,
            var_total_entries_dr,var_user_id,'U',NULL,NULL,'N',var_user_id,sysdate,var_user_id,sysdate,1,'A','O','Y',NULL,NULL);
        commit;

        for n in get_account_listing loop

                var_account := n.account;

                if length(var_account) =  10 then

                    select branch_code,ccy into var_branch_code, var_ccy
                    from skyeubs.sttm_cust_account where cust_ac_no  = var_account;

                elsif length(var_account) =  12 then

                    var_branch_code := substr(var_account,1,3);
                    var_account := substr(var_account,4,9);

                    var_ccy := n.ccy;

                else

                    var_branch_code := l_orgin_brn;
                    var_ccy := n.ccy;

                end if;

                if var_ccy ='NGN' then

                    var_exch_rate := null;

                    var_lcy_equi_amount := n.amount;


                else

                    var_exch_rate := n.rate;

                    var_lcy_equi_amount := round(n.amount * var_exch_rate,2);

                end if;

                INSERT INTO detb_upload_detail
                VALUES('',var_current_cycle,var_current_period,NULL,NULL,n.narration,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, var_account,
                NULL,NULL,NULL,NULL,p_batch_no ,NULL,NULL,l_orgin_brn,'DE_UPLOAD',var_knt,'O',var_ccy,var_dat_process, n.amount,
                var_account,var_branch_code,'CHG',n.crdr_ind,var_lcy_equi_amount,var_exch_rate,to_date(n.value_dt,'yyyymmdd'),'',NULL,NULL,NULL,NULL,null);
                commit;

                var_knt := var_knt + 1;

        end loop;


        INSERT INTO DETB_BATCH_RESTRICT_DETAIL values(l_orgin_brn,'DE_UPLOAD',p_batch_no,'ok');
        commit;

        skyeubs.autodeupload(p_batch_no,l_orgin_brn,'SYSTEM','Y');


        update xml_tab_master set batch_end = sysdate
        where batch_no  = p_batch_no;
        commit;

    end;



end;
