create or replace table ah_raw.bidb_airflow.mv_claim_details (
        claim_id varchar,
        cockpit_link varchar,
        current_claim_state varchar,
        claim_created_date varchar,
        track varchar,
        regulation varchar,
        is_extra_expenses varchar,
        claimants_email varchar,
        locale varchar,
        claimants_phone_number varchar,
        claimants_name varchar,
        claimants_address_country_code varchar,
        legal_entity varchar,
        is_gmbh varchar,
        vat_applicable varchar,
        cockpit_channel varchar,
        acquisition_channel varchar,
        ota_brand_name varchar,
        ota_name varchar,
        airline_name varchar,
        airline_currently_operating varchar,
        airline_identifier varchar,
        marketing_airline_identifier varchar,
        marketing_airline_name varchar,
        first_departure_airport varchar,
        itinerary_arrival_airport varchar,
        flight_number varchar,
        flight_date varchar,
        fellow_pax_name varchar,
        number_of_legs varchar,
        fast_track varchar,
        slow_track varchar,
        backoffice_id varchar,
        ahplus varchar,
        airline_reason varchar,
        airline_reference_number varchar,
        booking_ref varchar,
        claim_type varchar,
        contacted_airline_before varchar,
        delay varchar,
        geoip_country_code varchar,
        final_compensation varchar,
        number_of_passengers varchar,
        claim_closure_date varchar,
        claim_closure_reason varchar,
        claim_closure_state varchar,
        claim_closure_comment varchar,
        claim_assessment_date varchar,
        claim_assessment_status varchar,
        claim_assessment_rejection_reason varchar,
        claim_assessment_agent varchar,
        airline_submission_date varchar,
        airline_submission_method varchar,
        airline_submission_agent varchar,
        airline_response_date varchar,
        airline_response varchar,
        airline_rejection_reason varchar,
        airline_assessment_agent varchar,
        airline_late_acceptance_date varchar,
        jurisdiction_type varchar,
        jur_1_airline_hq varchar,
        jur_2_first_departure_country varchar,
        jur_3_itinerary_arrival_airport_country varchar,
        jur_4_country_of_the_client varchar,
        legal_assessment_date varchar,
        legal_assessment_status varchar,
        legal_assessment_rejection_reason varchar,
        legal_assessment_jurisdiction varchar,
        legal_assessment_agent varchar,
        assigned_external_lawyer varchar,
        legal_action_started_date varchar,
        legal_result_date varchar,
        legal_result varchar,
        legal_result_reason varchar,
        legal_result_agent varchar,
        last_legal_action_date varchar,
        last_legal_action_type varchar,
        last_selected_jurisdiction varchar,
        limitation_date varchar,
        legal_action_on_hold_reason varchar,
        legal_action_on_hold_reason_comment varchar,
        legal_action_on_hold_date varchar,
        legal_action_document_collection varchar,
        collected_compensation varchar,
        last_compensation_received_at varchar,
        compensation_collection_date varchar,
        last_compensation_account varchar,
        overdue_days varchar,
        overdue_compensation varchar,
        legal_result_comment varchar,
        last_lba_date varchar,
        standard_service_fee_amount varchar,
        legal_action_fee_amount varchar,
        total_revenue varchar,
        payout_date varchar,
        payout_status varchar,
        gross_amount_to_customer varchar,
        payout_method_selected_by_customer varchar,
        minor_pax varchar,
        last_document_request_date varchar,
        last_document_request_stage varchar,
        last_document_request_type varchar,
        service_fee_percentage varchar,
        legal_action_fee_percentage varchar,
        total_fee_percentage varchar,
        reprocessing_initiatives_count varchar,
        reprocessing_initiatives_list varchar,
        legally_viable_on_hold_reason varchar,
        assessed_amount varchar,
        client_stated_amount varchar,
        final_amount varchar,
        split_out_amount varchar,
        split_parent_or_child varchar
    );
copy into ah_raw.bidb_airflow.mv_claim_details
from (
        select $1:claim_id,
            $1:cockpit_link,
            $1:current_claim_state,
            $1:claim_created_date,
            $1:track,
            $1:regulation,
            $1:is_extra_expenses,
            md5($1:claimants_email),
            $1:locale,
            md5($1:claimants_phone_number),
            md5($1:claimants_name),
            $1:claimants_address_country_code,
            $1:legal_entity,
            $1:is_gmbh,
            $1:vat_applicable,
            $1:cockpit_channel,
            $1:acquisition_channel,
            $1:ota_brand_name,
            $1:ota_name,
            $1:airline_name,
            $1:airline_currently_operating,
            $1:airline_identifier,
            $1:marketing_airline_identifier,
            $1:marketing_airline_name,
            $1:first_departure_airport,
            $1:itinerary_arrival_airport,
            $1:flight_number,
            $1:flight_date,
            md5($1:fellow_pax_name),
            $1:number_of_legs,
            $1:fast_track,
            $1:slow_track,
            $1:backoffice_id,
            $1:ahplus,
            $1:airline_reason,
            $1:airline_reference_number,
            $1:booking_ref,
            $1:claim_type,
            $1:contacted_airline_before,
            $1:delay,
            $1:geoip_country_code,
            $1:final_compensation,
            $1:number_of_passengers,
            $1:claim_closure_date,
            $1:claim_closure_reason,
            $1:claim_closure_state,
            $1:claim_closure_comment,
            $1:claim_assessment_date,
            $1:claim_assessment_status,
            $1:claim_assessment_rejection_reason,
            $1:claim_assessment_agent,
            $1:airline_submission_date,
            $1:airline_submission_method,
            $1:airline_submission_agent,
            $1:airline_response_date,
            $1:airline_response,
            $1:airline_rejection_reason,
            $1:airline_assessment_agent,
            $1:airline_late_acceptance_date,
            $1:jurisdiction_type,
            $1:jur_1_airline_hq,
            $1:jur_2_first_departure_country,
            $1:jur_3_itinerary_arrival_airport_country,
            $1:jur_4_country_of_the_client,
            $1:legal_assessment_date,
            $1:legal_assessment_status,
            $1:legal_assessment_rejection_reason,
            $1:legal_assessment_jurisdiction,
            $1:legal_assessment_agent,
            $1:assigned_external_lawyer,
            $1:legal_action_started_date,
            $1:legal_result_date,
            $1:legal_result,
            $1:legal_result_reason,
            $1:legal_result_agent,
            $1:last_legal_action_date,
            $1:last_legal_action_type,
            $1:last_selected_jurisdiction,
            $1:limitation_date,
            $1:legal_action_on_hold_reason,
            $1:legal_action_on_hold_reason_comment,
            $1:legal_action_on_hold_date,
            $1:legal_action_document_collection,
            $1:collected_compensation,
            $1:last_compensation_received_at,
            $1:compensation_collection_date,
            $1:last_compensation_account,
            $1:overdue_days,
            $1:overdue_compensation,
            $1:legal_result_comment,
            $1:last_lba_date,
            $1:standard_service_fee_amount,
            $1:legal_action_fee_amount,
            $1:total_revenue,
            $1:payout_date,
            $1:payout_status,
            $1:gross_amount_to_customer,
            $1:payout_method_selected_by_customer,
            $1:minor_pax,
            $1:last_document_request_date,
            $1:last_document_request_stage,
            $1:last_document_request_type,
            $1:service_fee_percentage,
            $1:legal_action_fee_percentage,
            $1:total_fee_percentage,
            $1:reprocessing_initiatives_count,
            $1:reprocessing_initiatives_list,
            $1:legally_viable_on_hold_reason,
            $1:assessed_amount,
            $1:client_stated_amount,
            $1:final_amount,
            $1:split_out_amount,
            $1:split_parent_or_child
        from '@ah_raw.public.ah_raw_dt_production/bidb/public/mv_claim_details/dt=2023-06-14/'
    ) file_format = (
        format_name = ah_raw.public.parquet_dt_production
    ) on_error = 'skip_file';