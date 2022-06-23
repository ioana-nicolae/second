package power

const getNucPowerDealListQuery = `SELECT DISTINCT pd.power_key,
				pd.dlt_deal_type AS deal_type,
				pd.dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				pvm.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS PrtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.tz_time_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.option_key,
				pd.create_user AS createdBy,
				pd.create_date,
				pd.modify_user AS modifiedBy,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time,
				nvl(ef.df_field_value,'NA') AS exotic_flag
			FROM nucdba.power_deals pd
			INNER JOIN nucdba.companies c
				ON pd.cy_company_key = c.company_key
			INNER JOIN nucdba.portfolios p
				ON pd.prt_portfolio = p.portfolio
			LEFT OUTER JOIN nucdba.flat_broker_fees fbf
				ON pd.power_key = fbf.deal_key
					AND pd.dlt_deal_type = fbf.dlt_deal_type
			LEFT OUTER JOIN nucdba.companies bc
				ON fbf.cy_broker_key = bc.company_key
			INNER JOIN nucdba.companies l
				ON pd.lgl_cy_entity_key = l.company_key
			LEFT OUTER JOIN nucdba.contracts cn
				ON pd.kk_contract_key = cn.contract_key
			INNER JOIN nucdba.power_volume_months pvm
				ON pd.power_key = pvm.PV_PD_POWER_KEY
			LEFT OUTER JOIN nucdba.df_deal_attributes df
				ON pd.power_key = df.deal_key
					AND pd.dlt_deal_type = df.dlt_deal_type
					AND df.df_field_name = 'EXECUTION_DATE'
			LEFT OUTER JOIN nucdba.df_deal_attributes tf
				ON pd.power_key = tf.deal_key
					AND pd.dlt_deal_type = tf.dlt_deal_type
					AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
			LEFT OUTER JOIN nucdba.df_deal_attributes ef
				ON pd.power_key = ef.deal_key
					AND pd.dlt_deal_type = ef.dlt_deal_type
					AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
			WHERE pd.trade_date = :tradeDate
			AND (pd.modify_date > :lastRunTime
				or (1 IN (SELECT 1 FROM nucdba.power_volumes pv WHERE pv.pd_power_key = pd.power_key
				AND pv.modify_date > :lastRunTime))
				or fbf.modify_date > :lastRunTime)
			order by pd.power_key`

func getNucPowerTradeTermModelQuery(whereQuery string) string {
	return `SELECT pv.pd_power_key,
				pv.volume_seq,
				pv.dy_beg_day,
				pv.dy_end_day,
				pv.price_type,
				pv.price,
				pv.volume,
				pv.ppep_pp_pool,
				pv.ppep_pep_product,
				pv.ctp_point_code,
				pv.formula,
				pv.sch_schedule
			FROM nucdba.power_volumes pv
			WHERE ` + whereQuery
}

func getNucPowerTradeIndexModelQuery(whereQuery string) string {
	return `SELECT pv_pd_power_key,
				pv_volume_seq,
				pif_pi_pb_publication AS publication,
				pif_pi_pub_index AS pub_index,
				pif_frq_frequency AS frequency
			FROM nucdba.power_volume_indexes
			WHERE ` + whereQuery
}

const getNucPowerSwapDealListQuery = `SELECT DISTINCT pd.pswap_key,
				pd.dlt_deal_type AS deal_type,
				(SELECT sum(abs(m.volume)) FROM nucdba.power_swap_months m WHERE m.pswap_key = pd.pswap_key) AS total_quantity,
				CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				psm.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS PrtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.ib_prt_portfolio,
				ip.description AS ib_portfolio,
				pd.ib_ur_trader,
				pd.tz_time_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.option_key,
				pd.ppep_pp_pool,
				pd.ppep_pep_product,
				pd.volume,
				pd.fixed_price,
				pd.nonstd_flag,
				pd.pi_pb_publication,
				pd.pi_pub_index,
				pd.frq_frequency,
				pd.fix_pi_pb_publication,
				pd.fix_pi_pub_index,
				pd.fix_frq_frequency,
				pd.dy_beg_day,
				pd.dy_end_day,
				pd.sch_schedule,
				pd.create_user AS createdBy,
				pd.create_date,
				pd.modify_user AS modifiedBy,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time,
				NVL(ef.df_field_value,'NA') AS exotic_flag
			FROM nucdba.power_swaps pd
				INNER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				INNER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.pswap_key = fbf.deal_key
						AND pd.dlt_deal_type = fbf.dlt_deal_type
				INNER JOIN nucdba.power_swap_months psm
					ON pd.pswap_key = psm.pswap_key
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key = bc.company_key
				LEFT OUTER JOIN nucdba.portfolios ip
					ON pd.ib_prt_portfolio = ip.portfolio
				INNER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.pswap_key = df.deal_key
						AND pd.dlt_deal_type = df.dlt_deal_type
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.pswap_key = tf.deal_key
						AND pd.dlt_deal_type = tf.dlt_deal_type
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
				LEFT OUTER JOIN nucdba.df_deal_attributes ef
					ON pd.pswap_key = ef.deal_key
						AND pd.dlt_deal_type = ef.dlt_deal_type
						AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
			WHERE pd.trade_date = :tradeDate
				AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

func getNucPowerSwapDealTermModelQuery(whereQuery string) string {
	return `SELECT pswp_pswap_key,
				volume_seq,
				dy_beg_day,
				dy_end_day
			FROM nucdba.power_swap_volumes
			WHERE ` + whereQuery
}

const getNucPowerOptionsDealListQuery = `SELECT DISTINCT pd.poption_key,
					'POPTS' deal_type,
					(SELECT sum(abs(m.volume)) FROM nucdba.power_option_months m WHERE m.poption_key = pd.poption_key) AS total_quantity,
					CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
					pd.trade_date AS transaction_date,
					pd.cy_company_key,
					c.short_name AS company,
					c.long_name AS companylongname,
					nvl(c.company_code,c.short_name) AS companycode,
					l.short_name AS legalentity,
					l.LONG_NAME AS legalentitylongname,
					pd.lgl_cy_entity_key AS cylegalentitykey,
					CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
					cn.contract_number AS contractnumber,
					pd.cf_confirm_format AS confirmformat,
					pom.gr_region AS region,
					pd.hs_hedge_key,
					pd.prt_portfolio AS PrtPortfolio,
					p.description AS portfolio,
					pd.ur_trader,
					pd.ib_prt_portfolio,
					ip.description AS ib_portfolio,
					pd.ib_ur_trader,
					pd.tz_time_zone,
					pd.tz_exercise_zone,
					(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
					nvl(bc.short_name,'NA') AS broker,
					pd.ppep_pp_pool,
					pd.ppep_pep_product,
					pd.ctp_point_code,
					pd.settle_formula,
					pd.dy_beg_day,
					pd.dy_end_day,
					pd.sch_schedule,
					pd.volume,
					pd.strike_price,
					pd.strike_price_type,
					pd.strike_formula,
					pd.create_user AS createdBy,
					pd.modify_user AS modifiedBy,
					pd.create_date,
					pd.modify_date,
					df.df_field_value AS execution_date,
					tf.df_field_value AS execution_time,
					NVL(ef.df_field_value,'NA') AS exotic_flag
				FROM nucdba.power_options pd
					INNER JOIN nucdba.companies c
						ON pd.cy_company_key = c.company_key
					INNER JOIN nucdba.portfolios p
						ON pd.prt_portfolio = p.portfolio
					LEFT OUTER JOIN nucdba.flat_broker_fees fbf
						ON pd.poption_key = fbf.deal_key
							AND fbf.dlt_deal_type = 'POPTS'
					INNER JOIN nucdba.power_option_months pom
						ON pom.poption_key = pd.poption_keY
					LEFT OUTER JOIN nucdba.companies bc
						ON fbf.cy_broker_key = bc.company_key
					LEFT OUTER JOIN nucdba.portfolios ip
						ON pd.ib_prt_portfolio = ip.portfolio
					INNER JOIN nucdba.companies l
						ON pd.lgl_cy_entity_key = l.company_key
					LEFT OUTER JOIN nucdba.contracts cn
						ON pd.kk_contract_key = cn.contract_key
					LEFT OUTER JOIN nucdba.df_deal_attributes df
						ON pd.poption_key = df.deal_key
							AND df.dlt_deal_type = 'POPTS'
							AND df.df_field_name = 'EXECUTION_DATE'
					LEFT OUTER JOIN nucdba.df_deal_attributes tf
						ON pd.poption_key = tf.deal_key
							AND tf.dlt_deal_type = 'POPTS'
							AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
					LEFT OUTER JOIN nucdba.df_deal_attributes ef
						ON pd.poption_key = ef.deal_key
							AND ef.dlt_deal_type = 'POPTS'
							AND ef.df_field_name  = 'EXOTIC_TRADE_FLAG'
				WHERE pd.trade_date = :tradeDate
					AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

const getNucCapacityDealListQuery = `SELECT DISTINCT pd.capacity_key,
        'CAPCTY' AS deal_type,
        (SELECT sum(abs(m.volume)) FROM nucdba.capacity_deal_months m WHERE m.cpd_capacity_key = pd.capacity_key) AS total_quantity,
        pd.dn_direction,
        pd.trade_date AS transaction_date,
        pd.cy_company_key,
        c.short_name AS company,
        c.long_name AS companylongname,
        nvl(c.company_code,c.short_name) AS companycode,
        l.short_name AS legalentity,
        l.LONG_NAME AS legalentitylongname,
        pd.lgl_cy_entity_key AS cylegalentitykey,
        CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
        cn.contract_number AS contractnumber,
        pd.cf_confirm_format AS confirmformat,
        cdm.gr_region AS region,
        pd.hs_hedge_key,
        pd.prt_portfolio AS PrtPortfolio,
        p.description AS portfolio,
        pd.ur_trader,
        pd.tz_time_zone,
        (CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
        nvl(bc.short_name,'NA') AS broker,
        pd.non_standard_flag,
        pd.price_type,
        pd.charge,
        pd.volume,
        pd.energy_formula,
        pd.ppcp_pp_pool,
        pd.ppcp_pcp_product,
        pd.ctp_point_code,
        pd.dy_beg_day,
        pd.dy_end_day,
        pd.sch_schedule,
        pd.create_user AS createdBy,
        pd.modify_user AS modifiedBy,
        pd.create_date,
        pd.modify_date,
        df.df_field_value AS execution_date,
        tf.df_field_value AS execution_time
    FROM nucdba.capacity_deals pd
        INNER JOIN nucdba.companies c
            ON pd.cy_company_key = c.company_key
        INNER JOIN nucdba.portfolios p
            ON pd.prt_portfolio = p.portfolio
        LEFT OUTER JOIN nucdba.flat_broker_fees fbf
            ON pd.capacity_key = fbf.deal_key
                AND fbf.dlt_deal_type = 'CAPCTY'
        LEFT OUTER JOIN nucdba.companies bc
            ON fbf.cy_broker_key = bc.company_key
        INNER JOIN nucdba.companies l
            ON pd.lgl_cy_entity_key = l.company_key
        LEFT OUTER JOIN nucdba.contracts cn
            ON pd.kk_contract_key = cn.contract_key
        INNER JOIN nucdba.capacity_deal_months cdm
            ON pd.capacity_key = cdm.cpd_capacity_key
        LEFT OUTER JOIN nucdba.df_deal_attributes df
            ON pd.capacity_key = df.deal_key
                AND df.dlt_deal_type = 'CAPCTY'
                AND df.df_field_name = 'EXECUTION_DATE'
        LEFT OUTER JOIN nucdba.df_deal_attributes tf
            ON pd.capacity_key = tf.deal_key
                AND tf.dlt_deal_type = 'CAPCTY'
                AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
    WHERE pd.trade_date = :tradeDate
        AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

func getNucCapacityDealTermModelQuery(whereQuery string) string {
	return `SELECT DISTINCT cpd_capacity_key,
				dy_beg_day,
				dy_end_day
			FROM nucdba.capacity_volume_ranges
				WHERE ` + whereQuery
}

func getNucCapacityDealIndexModelQuery(whereQuery string) string {
	return `SELECT cpd_capacity_key,
				pif_pi_pb_publication AS publication,
				pif_pi_pub_index AS pub_index,
				pif_frq_frequency AS frequency
			FROM nucdba.capacity_deal_indexes
			WHERE ` + whereQuery
}

const getNucPTPDealListQuery = `SELECT DISTINCT pd.ptp_key,
		'PTP' AS deal_type,
		'PURCHASE' AS dn_direction,
		pd.trade_date AS transaction_date,
		pd.cy_company_key,
		c.short_name AS company,
		c.long_name AS companylongname,
		nvl(c.company_code,c.short_name) AS companycode,
		l.short_name AS legalentity,
		l.LONG_NAME AS legalentitylongname,
		pd.lgl_cy_entity_key AS cylegalentitykey,
		cn.contract_number AS contractnumber,
		'' AS confirmformat,
		pm.gr_region AS region,
		pd.hs_hedge_key,
		pd.prt_portfolio AS PrtPortfolio,
		p.description AS portfolio,
		pd.ur_trader,
		pd.tz_time_zone,
		(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
		nvl(bc.short_name,'NA') AS broker,
		pd.dy_flow_day,
		pd.ppep_pp_pool,
		pd.ppep_pep_product,
		pd.da_pi_pb_publication AS publication1,
		pd.poi_pi_pub_index AS pub_index1,
		pd.rt_pi_pb_publication AS publication2,
		pd.pow_pi_pub_index AS pub_index2,
		pd.create_user AS createdBy,
		pd.modify_user AS modifiedBy,
		pd.create_date,
		pd.modify_date
		FROM nucdba.ptp_deals pd
		INNER JOIN nucdba.companies c
			ON pd.cy_company_key = c.company_key
		INNER JOIN nucdba.portfolios p
			ON pd.prt_portfolio = p.portfolio
		LEFT OUTER JOIN nucdba.flat_broker_fees fbf
			ON pd.ptp_key = fbf.deal_key
				AND fbf.dlt_deal_type = 'PTP'
		LEFT OUTER JOIN nucdba.companies bc
			ON fbf.cy_broker_key = bc.company_key
		INNER JOIN nucdba.companies l
			ON pd.lgl_cy_entity_key = l.company_key
		LEFT OUTER JOIN nucdba.contracts cn
			ON pd.kk_contract_key = cn.contract_key
		INNER JOIN nucdba.ptp_months pm
			ON pd.ptp_key = pm.ptp_key
		WHERE pd.trade_date = :tradeDate
			AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

const getNucEmissionDealListQuery = `SELECT DISTINCT pd.emission_key,
					'EMSSN' AS deal_type,
					pd.dn_direction,
					pd.trade_date AS transaction_date,
					pd.cy_company_key,
					c.short_name AS company,
					c.long_name AS companylongname,
					nvl(c.company_code,c.short_name) AS companycode,
					l.short_name AS legalentity,
					l.LONG_NAME AS legalentitylongname,
					pd.lgl_cy_entity_key AS cylegalentitykey,
					cn.contract_number AS contractnumber,
					pd.cf_confirm_format AS confirmformat,
					edm.gr_region AS region,
					pd.hs_hedge_key,
					pd.prt_portfolio AS PrtPortfolio,
					p.description AS portfolio,
					pd.ur_trader,
					NULL AS tz_time_zone,
					(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
					nvl(bc.short_name,'NA') AS broker,
					pd.create_user AS createdBy,
					pd.modify_user AS modifiedBy,
					pd.create_date,
					pd.modify_date,
					df.df_field_value AS execution_date,
					tf.df_field_value AS execution_time
				FROM nucdba.emission_deals pd
				INNER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				INNER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.emission_key = fbf.deal_key
						AND fbf.dlt_deal_type= 'EMSSN'
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key = bc.company_key
				INNER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				INNER JOIN nucdba.emission_volume_months edm
					ON pd.emission_key = edm.ev_ed_emission_key
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.emission_key = df.deal_key
						AND df.dlt_deal_type = 'EMSSN'
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.emission_key = tf.deal_key
						AND tf.dlt_deal_type = 'EMSSN'
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
				WHERE pd.trade_date = :tradeDate
				AND (pd.modify_date > :lastRunTime
					OR (1 IN
						(SELECT 1
							FROM nucdba.emission_volumes pv
							WHERE pv.ed_emission_key = pd.emission_key
								AND pv.modify_date > :lastRunTime))
					OR fbf.modify_date > :lastRunTime)`

func getNucEmissionDealListTermModelQuery(whereQuery string) string {
	return `SELECT pv.ed_emission_key,
				pv.volume_seq,
				pv.dy_beg_day,
				pv.dy_end_day,
				pv.price_type,
				pv.price,
				pv.volume,
				pv.epdt_emission_product,
				pv.ctp_point_code,
				pv.formula
			FROM nucdba.emission_volumes pv
			WHERE ` + whereQuery
}

const getNucEmissionOptionDealListQuery = `SELECT DISTINCT pd.eoption_key,
				'EMOPTS' AS deal_type,
				CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				pd.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS PrtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.tz_exercise_zone AS tz_time_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.ed_emission_key,
				pd.strike_price,
				pd.volume,
				pd.create_user AS createdBy,
				pd.modify_user AS modifiedBy,
				pd.create_date,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time
			FROM nucdba.emission_options pd
				INNER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				INNER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.eoption_key = fbf.deal_key
						AND fbf.dlt_deal_type = 'EMOPTS'
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key = bc.company_key
				INNER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key  = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.eoption_key = df.deal_key
						AND df.dlt_deal_type = 'EMOPTS'
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.eoption_key = tf.deal_key
						AND tf.dlt_deal_type = 'EMOPTS'
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
			WHERE pd.trade_date = :tradeDate
				AND (pd.modify_date > :lastRunTime
					OR (1 IN (
						SELECT 1
						FROM nucdba.emission_volumes pv
						WHERE pv.ed_emission_key = pd.eoption_key
					AND pv.modify_date > :lastRunTime))
					OR fbf.modify_date > :lastRunTime)`

func getNucEmissionOptionDealTermListQuery(whereQuery string) string {
	return `SELECT pv.ed_emission_key,
				pv.volume_seq,
				pv.dy_beg_day,
				pv.dy_end_day,
				pv.ctp_point_code,
				pv.epdt_emission_product
			FROM nucdba.emission_volumes pv
			WHERE ` + whereQuery
}

const getNucSpreadOptionsDealListQuery = `SELECT DISTINCT pd.spread_option_key,
				'SPDOPT' AS deal_type,
				CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				som.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS prtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.ib_prt_portfolio,
				ip.description AS ib_portfolio,
				pd.ib_ur_trader,
				NULL AS tz_time_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.dy_beg_day1,
				pd.dy_end_day1,
				pd.sch_schedule,
				pd.formula1,
				pd.formula2,
				pd.ppep_pp_pool1,
				pd.ppep_pp_pool2,
				pd.ppep_pep_product1,
				pd.ppep_pep_product2,
				pd.volume,
				pd.strike_price,
				pd.create_user AS createdBy,
				pd.modify_user AS modifiedBy,
				pd.create_date,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time,
				NVL(ef.df_field_value,'NA') AS exotic_flag,
				NVL(pd.ctp_point_code1,'NOT APPLICABLE') AS point_code
			FROM nucdba.spread_options pd
				INNER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				INNER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.spread_option_key = fbf.deal_key
						AND fbf.dlt_deal_type = 'SPDOPT'
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key = bc.company_key
				LEFT OUTER JOIN nucdba.portfolios ip
					ON pd.ib_prt_portfolio = ip.portfolio
				LEFT OUTER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				LEFT OUTER JOIN nucdba.spread_option_months som
					ON pd.spread_option_key = som.spread_option_key
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.spread_option_key = df.deal_key
						AND df.dlt_deal_type = 'SPDOPT'
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.spread_option_key = tf.deal_key
						AND tf.dlt_deal_type = 'SPDOPT'
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
				LEFT OUTER JOIN nucdba.df_deal_attributes ef
					ON pd.spread_option_key = ef.deal_key
						AND ef.dlt_deal_type = 'SPDOPT'
						AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
			WHERE pd.trade_date = :tradeDate
				AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

const getNucHeatRateSwapsDealListQuery = `SELECT DISTINCT pd.hrswps_key,
			'HRSWPS' AS deal_type,
			(SELECT sum(abs(m.volume1 + m.r_volume1)) FROM nucdba.heat_rate_swap_months m WHERE m.hrswps_key = pd.hrswps_key) AS total_quantity,
			CASE WHEN sign(pd.volume1) = 1 THEN 'PURCHASE' WHEN sign(pd.volume1) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
			pd.trade_date AS transaction_date,
			pd.cy_company_key,
			c.short_name AS company,
			c.long_name AS companylongname,
			nvl(c.company_code,c.short_name) AS companycode,
			l.short_name AS legalentity,
			l.LONG_NAME AS legalentitylongname,
			pd.lgl_cy_entity_key AS cylegalentitykey,
			CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
			cn.contract_number AS contractnumber,
			pd.cf_confirm_format AS confirmformat,
			hsm.gr_region AS region,
			pd.hs_hedge_key,
			pd.prt_portfolio AS prtPortfolio,
			p.description AS portfolio,
			pd.ur_trader,
			NULL AS tz_time_zone,
			(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
			nvl(bc.short_name,'NA') AS broker,
			pd.option_key,
			pd.ppep_pp_pool,
			pd.ppep_pep_product,
			pd.pif_pi_pb_publication1,
			pd.pif_pi_pub_index1,
			pd.pif_pi_pb_publication2,
			pd.pif_pi_pub_index2,
			pd.pif_frq_frequency2,
			pd.dy_beg_day,
			pd.dy_end_day,
			pd.sch_schedule,
			pd.volume1,
			pd.create_user AS createdBy,
			pd.modify_user AS modifiedBy,
			pd.create_date,
			pd.modify_date,
			df.df_field_value AS execution_date,
			tf.df_field_value AS execution_time
		FROM nucdba.heat_rate_swaps pd
			INNER JOIN nucdba.companies c
				ON pd.cy_company_key = c.company_key
			INNER JOIN nucdba.portfolios p
				ON pd.prt_portfolio = p.portfolio
			LEFT OUTER JOIN nucdba.flat_broker_fees fbf
				ON pd.hrswps_key = fbf.deal_key
					AND fbf.dlt_deal_type = 'HRSWPS'
			LEFT OUTER JOIN nucdba.companies bc
				ON fbf.cy_broker_key = bc.company_key
			INNER JOIN nucdba.companies l
				ON pd.lgl_cy_entity_key = l.company_key
			LEFT OUTER JOIN nucdba.contracts cn
				ON pd.kk_contract_key = cn.contract_key
			INNER JOIN nucdba.heat_rate_swap_months hsm
				ON hsm.hrswps_key = pd.hrswps_key
			LEFT OUTER JOIN nucdba.df_deal_attributes df
				ON pd.hrswps_key = df.deal_key
					AND df.dlt_deal_type = 'HRSWPS'
					AND df.df_field_name = 'EXECUTION_DATE'
			LEFT OUTER JOIN nucdba.df_deal_attributes tf
				ON pd.hrswps_key = tf.deal_key
					AND tf.dlt_deal_type = 'HRSWPS'
					AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
		WHERE pd.trade_date = :tradeDate
			AND (pd.modify_date > :lastRunTime or fbf.modify_date > :lastRunTime)`

const getNucTCCFTRSDealListQuery = `SELECT DISTINCT pd.deal_key,
			pd.dlt_deal_type AS deal_type,
			CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
			pd.trade_date AS transaction_date,
			pd.cy_company_key,
			c.short_name AS company,
			c.long_name AS companylongname,
			nvl(c.company_code,c.short_name) AS companycode,
			l.short_name AS legalentity,
			l.LONG_NAME AS legalentitylongname,
			pd.lgl_cy_entity_key AS cylegalentitykey,
			cn.contract_number AS contractnumber,
			pd.cf_confirm_format AS confirmformat,
			itm.gr_region AS region,
			pd.hs_hedge_key,
			pd.prt_portfolio AS prtPortfolio,
			p.description AS portfolio,
			pd.ur_trader,
			pd.tz_time_zone,
			(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
			nvl(bc.short_name,'NA') AS broker,
			pd.dy_beg_day,
			pd.dy_end_day,
			pd.sch_schedule,
			pd.volume,
			pd.fixed_price,
			pd.ppep_pp_pool,
			pd.ppep_pep_product,
			pd.pi_pb_publication,
			pd.frq_frequency,
			pd.poi_pi_pub_index,
			pd.pow_pi_pub_index,
			pd.create_user AS createdBy,
			pd.modify_user AS modifiedBy,
			pd.create_date,
			pd.modify_date
		FROM nucdba.iso_tccftrs pd
			INNER JOIN nucdba.companies c
				ON pd.cy_company_key = c.company_key
			INNER JOIN nucdba.portfolios p
				ON pd.prt_portfolio = p.portfolio
			LEFT OUTER JOIN nucdba.flat_broker_fees fbf
				ON pd.deal_key = fbf.deal_key
					AND pd.dlt_deal_type = fbf.dlt_deal_type
			LEFT OUTER JOIN nucdba.companies bc
				ON fbf.cy_broker_key = bc.company_key
			INNER JOIN nucdba.companies l
				ON pd.lgl_cy_entity_key = l.company_key
			LEFT OUTER JOIN nucdba.contracts cn
				ON pd.kk_contract_key = cn.contract_key
			INNER JOIN nucdba.iso_tccftr_months itm
				ON itm.deal_key = pd.deal_key
		WHERE pd.trade_date = :tradeDate
			AND pd.dlt_deal_type = :strDealType
			AND (pd.modify_date > :lastRunTime OR fbf.modify_date > :lastRunTime)`

const getNucTransmissionDealListQuery = `SELECT DISTINCT pd.trans_key,
			'TRANS' AS deal_type,
			pd.dn_direction,
			pd.trade_date AS transaction_date,
			pd.cy_company_key,
			c.short_name AS company,
			c.long_name AS companylongname,
			nvl(c.company_code,c.short_name) AS companycode,
			l.short_name AS legalentity,
			l.LONG_NAME AS legalentitylongname,
			pd.lgl_cy_entity_key AS cylegalentitykey,
			cn.contract_number AS contractnumber,
			pd.cf_confirm_format AS confirmformat,
			tvm.gr_fm_region AS region,
			pd.hs_hedge_key,
			pd.prt_portfolio AS prtPortfolio,
			p.description AS portfolio,
			pd.ur_trader,
			pd.tz_time_zone,
			(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) has_broker,
			nvl(bc.short_name,'NA') AS broker,
			pd.create_user AS createdBy,
			pd.modify_user AS modifiedBy,
			pd.create_date,
			pd.modify_date,
			NVL(df.df_field_value,'01/01/1900') AS execution_time
		FROM nucdba.transmission_deals pd
			INNER JOIN nucdba.companies c
				ON pd.cy_company_key = c.company_key
			INNER JOIN nucdba.portfolios p
				ON pd.prt_portfolio = p.portfolio
			LEFT OUTER JOIN nucdba.flat_broker_fees fbf
				ON pd.trans_key = fbf.deal_key
					AND fbf.dlt_deal_type = 'TRANS'
			LEFT OUTER JOIN nucdba.companies bc
				ON fbf.cy_broker_key = bc.company_key
			INNER JOIN nucdba.companies l
				ON pd.lgl_cy_entity_key  = l.company_key
			LEFT OUTER JOIN nucdba.contracts cn
				ON pd.kk_contract_key = cn.contract_key
			INNER JOIN nucdba.trans_volume_months tvm
				ON tvm.tv_td_trans_key = pd.trans_key
			LEFT OUTER JOIN nucdba.df_deal_attributes df
				ON pd.trans_key = df.deal_key
					AND df.dlt_deal_type = 'TRANS'
					AND df.df_field_name = 'EXECUTION_TIMESTAMP'
		WHERE pd.trade_date = :tradeDate
			AND (pd.modify_date > :lastRunTime
				OR (1 IN (SELECT 1
							FROM nucdba.trans_volumes pv
							WHERE pv.td_trans_key = pd.trans_key
								AND pv.modify_date > :lastRunTime))
				OR fbf.modify_date > :lastRunTime)`

func getNucTransmissionDealTermListQuery(whereQuery string) string {
	return `SELECT pv.td_trans_key,
					pv.volume_seq,
					pv.dy_beg_day,
					pv.dy_end_day,
					pv.volume,
					pv.ppep_pep_product,
					pv.ppep_pp_fm_pool,
					pv.ctp_fm_point_code,
					pv.ppep_pp_to_pool,
					pv.ctp_to_point_code,
					pv.sch_schedule
				FROM nucdba.trans_volumes pv
				WHERE ` + whereQuery
}

const getNucMiscChargeDealListQuery = `SELECT DISTINCT pd.misc_charge_key,
			'MISC' AS deal_type,
			decode(pd.rec_pay_flag,'P','Payable','Receivable') AS dn_direction,
			pd.trade_date AS transaction_date,
			pd.cy_company_key,
			c.short_name AS company,
			c.long_name AS companylongname,
			nvl(c.company_code,c.short_name) AS companycode,
			l.short_name AS legalentity,
			l.LONG_NAME AS legalentitylongname,
			pd.lgl_cy_entity_key AS cylegalentitykey,
			cn.contract_number AS contractnumber,
			'' AS confirmformat,
			'' AS region,
			pd.hs_hedge_key,
			pd.prt_portfolio AS prtPortfolio,
			p.description AS portfolio,
			pd.ur_trader,
			'' AS tz_time_zone,
			'' AS has_broker,
			'' AS broker,
			pd.create_user AS createdBy,
			pd.modify_user AS modifiedBy,
			pd.create_date,
			pd.modify_date
		FROM nucdba.misc_charges pd
			INNER JOIN nucdba.companies c
				ON pd.cy_company_key = c.company_key
			INNER JOIN nucdba.portfolios p
				ON pd.prt_portfolio = p.portfolio
			INNER JOIN nucdba.companies l
				ON pd.lgl_cy_entity_key = l.company_key
			LEFT OUTER JOIN nucdba.contracts cn
				ON pd.kk_contract_key = cn.contract_key
		WHERE pd.trade_date = :tradeDate
			AND pd.modify_date > :lastRunTime`

func getNucMiscChargeDealTermListQuery(whereQuery string) string {
	return `SELECT pv.mc_misc_charge_key,
					pv.misc_vol_seq,
					pv.dy_beg_day,
					pv.dy_end_day,
					pv.int_volume
				FROM nucdba.misc_charge_volumes pv
				WHERE ` + whereQuery
}

const execProcessTradesQuery = "EXEC dbo.SP_PROCESS_NUCLEUSTRADES_UPSERT @TVP;"

const getLastExtractionRunQuery = `SELECT ExtractionRunId, TransactionDate, DealType, TimeParameter, CreatedAt
								FROM dbo.NucleusTradeExtractionRun
								WHERE TransactionDate = @transactionDate
									AND DealType = @dealType
									AND ExtractionRunId = (SELECT MAX(ExtractionRunId)
															FROM dbo.NucleusTradeExtractionRun
															WHERE TransactionDate = @transactionDate
																AND DealType = @dealType);`

const insertExtractionRunQuery = `INSERT INTO dbo.NucleusTradeExtractionRun
									(TransactionDate, DealType, TimeParameter, CreatedAt)
								VALUES
									(@transactionDate, @dealType, @timeParameter, SYSUTCDATETIME());`

const getPortfolioRiskMappingListQuery = `SELECT SourceSystem, Portfolio, LegalEntity
										FROM dbo.PortfolioRiskMapping
										WHERE SourceSystem = 'NUCLEUS';`

const getLarBaselistQuery = `SELECT lb.ShortName,
								lb.CounterpartyLongName,
								ParentCompany,
								Product,
								SourceSystem,
								DealType,
								NettingAgreement,
								AgreementTypePerCSA,
								OurThreshold,
								CounterpartyThreshold,
								BuyTenor,
								SellTenor,
								GrossExposure,
								Collateral,
								NetPosition,
								LimitValue,
								LimitCurrency,
								LimitAvailability,
								ExposureLimit,
								ExpirationDate,
								MarketType,
								IndustryCode,
								SPRating,
								MoodyRating,
								FinalInternalRating,
								FinalRating,
								Equifax,
								AmendedBy,
								EffectiveDate,
								ReviewDate,
								DoddFrankClassification,
								ReportCreatedDate,
								Boost,
								TradingEntity,
								lps.legalentity AS LegalEntity,
								Agmt,
								CSA,
								tenor,
								Limit,
								ReportingDate,
								CreatedAt
							FROM dbo.LarBase lb
								LEFT JOIN dbo.LarProductSourceRef lps
									ON lb.Product = lps.ProductName
										AND lps.sourcesystem = 'NUCLEUS'
							WHERE ReportingDate = (SELECT isnull(max(ReportingDate), getdate())
													FROM dbo.LarBase);`

func getNucPowerDealByKeysQuery(inWhereQuery string) string {
	return `SELECT DISTINCT pd.power_key,
					pd.dlt_deal_type AS deal_type,
					pd.dn_direction,
					pd.trade_date AS transaction_date,
					pd.cy_company_key,
					c.short_name AS company,
					c.long_name AS companylongname,
					nvl(c.company_code,c.short_name) AS companycode,
					l.short_name AS legalentity,
					l.LONG_NAME AS legalentitylongname,
					pd.lgl_cy_entity_key AS cylegalentitykey,
					cn.contract_number AS contractnumber,
					pd.cf_confirm_format AS confirmformat,
					pvm.gr_region AS region,
					pd.hs_hedge_key,
					pd.prt_portfolio AS prtPortfolio,
					p.description AS portfolio,
					pd.ur_trader,
					pd.tz_time_zone,
					(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
					nvl(bc.short_name,'NA') AS broker,
					pd.option_key,
					pd.create_user AS createdBy,
					pd.create_date,
					pd.modify_user AS modifiedBy,
					pd.modify_date,
					df.df_field_value AS execution_date,
					tf.df_field_value AS execution_time,
					nvl(ef.df_field_value,'NA') AS exotic_flag
				FROM nucdba.power_deals pd
					INNER JOIN nucdba.companies c
						ON pd.cy_company_key = c.company_key
					INNER JOIN nucdba.portfolios p
						ON pd.prt_portfolio = p.portfolio
					LEFT OUTER JOIN nucdba.flat_broker_fees fbf
						ON pd.power_key = fbf.deal_key
							AND pd.dlt_deal_type = fbf.dlt_deal_type
					LEFT OUTER JOIN nucdba.companies bc
						ON fbf.cy_broker_key = bc.company_key
					INNER JOIN nucdba.companies l
						ON pd.lgl_cy_entity_key = l.company_key
					LEFT OUTER JOIN nucdba.contracts cn
						ON pd.kk_contract_key = cn.contract_key
					INNER JOIN nucdba.power_volume_months pvm
						ON pvm.pv_pd_power_key = pd.power_key
					LEFT OUTER JOIN nucdba.df_deal_attributes df
						ON pd.power_key = df.deal_key
							AND pd.dlt_deal_type = df.dlt_deal_type
							AND df.df_field_name = 'EXECUTION_DATE'
					LEFT OUTER JOIN nucdba.df_deal_attributes tf
						ON pd.power_key = tf.deal_key
							AND pd.dlt_deal_type = tf.dlt_deal_type
							AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
					LEFT OUTER JOIN nucdba.df_deal_attributes ef
						ON pd.power_key = ef.deal_key
							AND pd.dlt_deal_type = ef.dlt_deal_type
							AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
				WHERE ` + inWhereQuery
}

func getNucPowerSwapDealByKeysQuery(inWhereQuery string) string {
	return `SELECT DISTINCT pd.pswap_key,
				pd.dlt_deal_type AS deal_type,
				(SELECT sum(abs(m.volume)) FROM nucdba.power_swap_months m WHERE m.pswap_key = pd.pswap_key) AS total_quantity,
				CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				psm.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS prtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.ib_prt_portfolio,
				ip.description AS ib_portfolio,
				pd.ib_ur_trader,
				pd.tz_time_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.option_key,
				pd.ppep_pp_pool,
				pd.ppep_pep_product,
				pd.volume,
				pd.fixed_price,
				pd.nonstd_flag,
				pd.pi_pb_publication,
				pd.pi_pub_index,
				pd.frq_frequency,
				pd.fix_pi_pb_publication,
				pd.fix_pi_pub_index,
				pd.fix_frq_frequency,
				pd.dy_beg_day,
				pd.dy_end_day,
				pd.sch_schedule,
				pd.create_user AS createdBy,
				pd.create_date,
				pd.modify_user AS modifiedBy,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time,
				NVL(ef.df_field_value,'NA') AS exotic_flag
			FROM nucdba.power_swaps pd
				INNER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				INNER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.pswap_key = fbf.deal_key
						AND pd.dlt_deal_type = fbf.dlt_deal_type
				INNER JOIN nucdba.power_swap_months psm
					ON pd.pswap_key = psm.pswap_key
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key = bc.company_key
				LEFT OUTER JOIN nucdba.portfolios ip
					ON pd.ib_prt_portfolio = ip.portfolio
				INNER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.pswap_key = df.deal_key
						AND pd.dlt_deal_type = df.dlt_deal_type
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.pswap_key = tf.deal_key
						AND pd.dlt_deal_type = tf.dlt_deal_type
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
				LEFT OUTER JOIN nucdba.df_deal_attributes ef
					ON pd.pswap_key = ef.deal_key
						AND pd.dlt_deal_type = ef.dlt_deal_type
						AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
			WHERE ` + inWhereQuery
}

func getNucPowerOptionsDealByKeysQuery(inWhereQuery string) string {
	return `SELECT DISTINCT pd.poption_key,
				'POPTS' AS deal_type,
				(SELECT sum(abs(m.volume)) FROM nucdba.power_option_months m WHERE m.poption_key = pd.poption_key) AS total_quantity,
				CASE WHEN sign(pd.volume) = 1 THEN 'PURCHASE' WHEN sign(pd.volume) = -1 THEN 'SALE' ELSE 'UNDETERMINED' END AS dn_direction,
				pd.trade_date AS transaction_date,
				pd.cy_company_key,
				c.short_name AS company,
				c.long_name AS companylongname,
				nvl(c.company_code,c.short_name) AS companycode,
				l.short_name AS legalentity,
				l.LONG_NAME AS legalentitylongname,
				pd.lgl_cy_entity_key AS cylegalentitykey,
				CASE WHEN c.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND l.short_name IN ('SENA','STRM','SCAN','SHECHE CAD') AND pd.lgl_cy_entity_key != pd.cy_company_key THEN 'Y' ELSE 'N' END AS interaffiliate_flag,
				cn.contract_number AS contractnumber,
				pd.cf_confirm_format AS confirmformat,
				pom.gr_region AS region,
				pd.hs_hedge_key,
				pd.prt_portfolio AS prtPortfolio,
				p.description AS portfolio,
				pd.ur_trader,
				pd.ib_prt_portfolio,
				ip.description AS ib_portfolio,
				pd.ib_ur_trader,
				pd.tz_time_zone,
				pd.tz_exercise_zone,
				(CASE WHEN fbf.cy_broker_key IS NULL THEN 'NO' ELSE 'YES' END) AS has_broker,
				nvl(bc.short_name,'NA') AS broker,
				pd.ppep_pp_pool,
				pd.ppep_pep_product,
				pd.ctp_point_code,
				pd.settle_formula,
				pd.dy_beg_day,
				pd.dy_end_day,
				pd.sch_schedule,
				pd.volume,
				pd.strike_price,
				pd.strike_price_type,
				pd.strike_formula,
				pd.create_user AS createdBy,
				pd.modify_user AS modifiedBy,
				pd.create_date,
				pd.modify_date,
				df.df_field_value AS execution_date,
				tf.df_field_value AS execution_time,
				NVL(ef.df_field_value,'NA') AS exotic_flag
			FROM nucdba.power_options pd
				LEFT OUTER JOIN nucdba.companies c
					ON pd.cy_company_key = c.company_key
				LEFT OUTER JOIN nucdba.portfolios p
					ON pd.prt_portfolio = p.portfolio
				LEFT OUTER JOIN nucdba.flat_broker_fees fbf
					ON pd.poption_key = fbf.deal_key
						AND fbf.dlt_deal_type = 'POPTS'
				LEFT OUTER JOIN power_option_months pom
					ON pd.poption_key = pom.poption_key
				LEFT OUTER JOIN nucdba.companies bc
					ON fbf.cy_broker_key =bc.company_key
				LEFT OUTER JOIN nucdba.companies l
					ON pd.lgl_cy_entity_key = l.company_key
				LEFT OUTER JOIN nucdba.contracts cn
					ON pd.kk_contract_key = cn.contract_key
				LEFT OUTER JOIN nucdba.portfolios ip
					ON pd.ib_prt_portfolio = ip.portfolio
				LEFT OUTER JOIN nucdba.df_deal_attributes df
					ON pd.poption_key = df.deal_key
						AND df.dlt_deal_type = 'POPTS'
						AND df.df_field_name = 'EXECUTION_DATE'
				LEFT OUTER JOIN nucdba.df_deal_attributes tf
					ON pd.poption_key = tf.deal_key
						AND tf.dlt_deal_type = 'POPTS'
						AND tf.df_field_name = 'EXECUTION_TIMESTAMP'
				LEFT OUTER JOIN nucdba.df_deal_attributes ef
					ON pd.poption_key = ef.deal_key
						AND ef.dlt_deal_type = 'POPTS'
						AND ef.df_field_name = 'EXOTIC_TRADE_FLAG'
			WHERE  ` + inWhereQuery
}
