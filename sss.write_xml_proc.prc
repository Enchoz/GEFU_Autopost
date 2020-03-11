CREATE OR REPLACE PROCEDURE write_xml_proc (reference_no varchar2, ref_desc varchar2, xml_input varchar2, response out varchar2)
as
    v_count number;
    v_sum number;
    v_response varchar2(1000);
    v_account varchar2 (20);
    v_narration varchar2 (250);
    v_crdr_ind char(1);
    v_amount number;
    v_comm number;
    v_ccy varchar2 (3);
    v_code number;
    v_rate number;
    v_value_dt number;

    var_batch_no varchar2(4);

    xml_data varchar2(5000) := xml_input;
    /*'<upload_trans>
                        <record>
                            <acct_no>1780159383</acct_no>
                            <narration>self</narration>
                            <crdr_ind>D</crdr_ind>m
                            <amount>2000</amount>
                            <comm>100</comm>
                            <ccy>NGN</ccy>
                            <code>35131</code>
                            <rate>10</rate>
                            <value_dt>20200221</value_dt>
                         </record>

                         <record>
                            <acct_no>1780065891</acct_no>
                            <narration>self</narration>
                            <crdr_ind>D</crdr_ind>
                            <amount>2400</amount>
                            <comm>100</comm>
                            <ccy>NGN</ccy>
                            <code>4544</code>
                            <rate>10</rate>
                            <value_dt>20200221</value_dt>
                         </record>
                        </upload_trans>';*/
    cursor records is
        with t (xml) as (
        select xmltype(xml_data) from dual
        )
        select x.acct_no, x.narration, x.crdr_ind, x.amount, x.comm, x.ccy, x.code, x.rate, x.value_dt --x.grade, y.name, y.company, x.group_num, x.designation, x.company_code
        from t
        cross join xmltable ('/upload_trans/record'
        passing t.xml
        columns acct_no varchar2(20) path 'acct_no',
        --row_xml xmltype path '.',
        narration varchar2(250) path 'narration',
        crdr_ind char(1) path 'crdr_ind',
        amount number path 'amount',
        comm number path 'comm',
        ccy varchar2(3) path 'ccy',
        code number path 'code',
        rate number path 'rate',
        value_dt number path 'value_dt'
        ) x
        /*cross join xmltable ('/row/Employee'
        passing x.row_xml
        columns name varchar2(30) path 'Name',
        company varchar2(5) path 'company'
        ) y*/;
begin
    select count(*)
    into v_count
    from xml_tab
    where file_name = reference_no;


    if v_count = 0 then


        for w in records loop

            insert into xml_tab(file_name, account, narration, crdr_ind, amount, comm, ccy, code, rate, value_dt)
            values (reference_no, w.acct_no, w.narration, w.crdr_ind, w.amount, w.comm, w.ccy, w.code, w.rate, w.value_dt);
            commit;

        end loop;

        select sum(decode(crdr_ind, 'D', -amount, amount))
        into v_sum
        from xml_tab
        where file_name = reference_no;

        if v_sum = 0 then

            select sss.get_next_batch() into var_batch_no from dual;

            v_response := var_batch_no || '|Successful';

            insert into xml_tab_master values (var_batch_no,reference_no,ref_desc,null,null);
            commit;

            bulk_xml_upload (var_batch_no);

        else
            delete from xml_tab
            where file_name = reference_no;
            commit;

            v_response := 'Error due to credit/debit mismatch';

        end if;


    else


        v_response := 'Reference number already exists';


    end if;


    response := v_response;
end;
