package power

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/sede-x/RogerRogerAnomalyDetector/db/oracle"
	"github.com/sede-x/RogerRogerAnomalyDetector/logger"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/common"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/nucleus"
)

func parseTimeLayout(format string, date string) time.Time {
	time, err := time.Parse(format, date)
	if err != nil {
		log.Println("parseTimeLayout err: ", err)
	}
	return time
}

func parseTime(date string) time.Time {
	return parseTimeLayout("02-01-2006", date)
}

func parseDateTime(date string, timestamp string) time.Time {
	if date == "" || timestamp == "" {
		return time.Time{}
	}
	executionTimeString := date + " " + timestamp
	execTime, err := time.Parse("2006-01-02 03:04:05 PM", executionTimeString)
	if err != nil {
		execTime, err = time.Parse("01/02/2006 03:04:05 PM", executionTimeString)
		if err != nil {
			log.Println("parseDateTime err: ", err)
		}
	}
	return execTime
}

type AnyValueMatch struct{}

func (e AnyValueMatch) Match(v driver.Value) bool {
	return true
}

func TestNucleusTradeRepository_GetNucPowerDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"POWER_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER",
		"CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER",
		"TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "OPTION_KEY", "CREATEDBY", "CREATE_DATE",
		"MODIFIEDBY", "MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME", "EXOTIC_FLAG"}

	mock.ExpectQuery(getNucPowerDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, "PWRNSD", "PURCHASE", parseTime("05-05-2022"),
			1601, "PJM", "PJM INTERCONNECTION LLC", "PJM",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "011-KW-BI-05701",
			"HOURLY", "EAST", "", 288, "SD - TRANS", "SROSS",
			"PPT", "NO", "NA", nil, "SROSS", parseTime("04-05-2022"),
			"SROSS", parseTime("05-05-2022"), "", "", "NA",
		).AddRow(
			3996507, "PWRNSD", "SALE", parseTime("05-05-2022"),
			10459, "MISO", "MIDWEST INDEPENDENT TRANSMISSION SYSTEM", "MIDWEST IN",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "011-KW-IS-10952",
			"HOURLY", "EAST", "", 288, "SD - TRANS", "SROSS",
			"PPT", "NO", "NA", nil, "SROSS", parseTime("04-05-2022"),
			"SROSS", parseTime("05-05-2022"), "2022-05-05", "08:18:55 AM", "NA",
		))

	dealKeysArray := []int{3996506, 3996507}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.pd_power_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerTradeTermModelQ := getNucPowerTradeTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"PD_POWER_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY", "PRICE_TYPE",
		"PRICE", "VOLUME", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT", "CTP_POINT_CODE", "FORMULA",
		"SCH_SCHEDULE"}

	mock.ExpectQuery(getNucPowerTradeTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, 0, parseTime("05-05-2022"), parseTime("05-05-2022"), "I",
			0, 0, "PJM", "HOURLY", "MISO", "[PJM DA LMP|40523629|HOURLY]",
			"NERC",
		).AddRow(
			3996507, 0, parseTime("05-05-2022"), parseTime("05-05-2022"), "I",
			0, 0, "MISOE", "HOURLY", "MISO/PJM", "[MISO DALMP|PJMC|HOURLY]",
			"NERC",
		))

	formulaMap := make(map[int]int)
	formulaMap[3996506] = 0
	formulaMap[3996507] = 0

	insNumQuery, _, err := oracle.CreateMapIntWhereQuery(formulaMap, []interface{}{}, "pv_pd_power_key", "pv_volume_seq")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerTradeIndexModelQ := getNucPowerTradeIndexModelQuery(insNumQuery)

	columns = []string{"PV_PD_POWER_KEY", "PV_VOLUME_SEQ", "PUBLICATION", "PUB_INDEX", "FREQUENCY"}

	// this is a small hack to allow any values to be in any order for the mock,
	// the issue is that the parameters for the query never will be in the same
	// order because it comes from iterating over the keys of a map and Go always
	// randmize them
	mock.ExpectQuery(getNucPowerTradeIndexModelQ).WithArgs(AnyValueMatch{}, AnyValueMatch{},
		AnyValueMatch{}, AnyValueMatch{}).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, 0, "PJM DA LMP", "40523629", "HOURLY",
		).AddRow(
			3996507, 0, "MISO DALMP", "PJMC", "HOURLY",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             3996506,
					DealType:            "PWRNSD",
					TotalQuantity:       0,
					TransactionDate:     parseTime("05-05-2022"),
					DnDirection:         "PURCHASE",
					CyCompanyKey:        1601,
					CompanyCode:         "PJM",
					Company:             "PJM",
					CompanyLongName:     "PJM INTERCONNECTION LLC",
					HsHedgeKey:          "",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "SROSS",
					CyBrokerKey:         0,
					Broker:              "NA",
					DaRtIndicator:       "",
					TzTimeZone:          "PPT",
					TzExerciseZone:      "",
					HasBroker:           "NO",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					ExercisedOptionKey:  0,
					Contract:            "011-KW-BI-05701",
					ConfirmFormat:       "HOURLY",
					CyLegalEntityKey:    10430,
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					Region:              "EAST",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
							Pool1:           "PJM",
							Product1:        "HOURLY",
							PointCode1:      "MISO",
							Pool2:           "",
							Product2:        "",
							HolidaySchedule: "NERC",
							PointCode2:      "",
							Formula1:        "[PJM DA LMP|40523629|HOURLY]",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									VolSeq:      0,
									Publication: "PJM DA LMP",
									PubIndex:    "40523629",
									Frequency:   "HOURLY",
								},
							},
							Formula2:   "",
							Indexes2:   nil,
							PriceType:  "I",
							FixedPrice: 0,
							Volume:     0,
						},
					},
					AnomalyTestResult:  "",
					CreatedAt:          parseTime("04-05-2022"),
					ModifiedAt:         parseTime("05-05-2022"),
					CreatedBy:          "SROSS",
					ModifiedBy:         "SROSS",
					OptionType:         "",
					ExecutionTime:      parseTime("01-01-0001"),
					ExoticFlag:         "NA",
					InteraffiliateFlag: "N",
					StartDate:          parseTime("01-01-0001"),
					EndDate:            parseTime("01-01-0001"),
				},
				{
					DealKey:             3996507,
					DealType:            "PWRNSD",
					TotalQuantity:       0,
					TransactionDate:     parseTime("05-05-2022"),
					DnDirection:         "SALE",
					CyCompanyKey:        10459,
					CompanyCode:         "MIDWEST IN",
					Company:             "MISO",
					CompanyLongName:     "MIDWEST INDEPENDENT TRANSMISSION SYSTEM",
					HsHedgeKey:          "",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "SROSS",
					CyBrokerKey:         0,
					Broker:              "NA",
					DaRtIndicator:       "",
					TzTimeZone:          "PPT",
					TzExerciseZone:      "",
					HasBroker:           "NO",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					ExercisedOptionKey:  0,
					Contract:            "011-KW-IS-10952",
					ConfirmFormat:       "HOURLY",
					CyLegalEntityKey:    10430,
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					Region:              "EAST",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
							Pool1:           "MISOE",
							Product1:        "HOURLY",
							PointCode1:      "MISO/PJM",
							Pool2:           "",
							Product2:        "",
							HolidaySchedule: "NERC",
							PointCode2:      "",
							Formula1:        "[MISO DALMP|PJMC|HOURLY]",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									VolSeq:      0,
									Publication: "MISO DALMP",
									PubIndex:    "PJMC",
									Frequency:   "HOURLY",
								},
							},
							Formula2:   "",
							Indexes2:   nil,
							PriceType:  "I",
							FixedPrice: 0,
							Volume:     0,
						},
					},
					AnomalyTestResult:  "",
					CreatedAt:          parseTime("04-05-2022"),
					ModifiedAt:         parseTime("05-05-2022"),
					CreatedBy:          "SROSS",
					ModifiedBy:         "SROSS",
					OptionType:         "",
					ExecutionTime:      parseDateTime("2022-05-05", "08:18:55 AM"),
					ExoticFlag:         "NA",
					InteraffiliateFlag: "N",
					StartDate:          parseTime("01-01-0001"),
					EndDate:            parseTime("01-01-0001"),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPowerSwapDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"PSWAP_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "IB_PRT_PORTFOLIO", "IB_PORTFOLIO", "IB_UR_TRADER",
		"TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "OPTION_KEY", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT",
		"VOLUME", "FIXED_PRICE", "NONSTD_FLAG", "PI_PB_PUBLICATION", "PI_PUB_INDEX",
		"FRQ_FREQUENCY", "FIX_PI_PB_PUBLICATION", "FIX_PI_PUB_INDEX", "FIX_FRQ_FREQUENCY",
		"DY_BEG_DAY", "DY_END_DAY", "SCH_SCHEDULE", "CREATEDBY", "CREATE_DATE", "MODIFIEDBY",
		"MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME", "EXOTIC_FLAG"}
	mock.ExpectQuery(getNucPowerSwapDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1388598, "PSWPS", 800, "PURCHASE",
			parseTime("05-05-2022"), 18098, "FIMAT USA", "NEWEDGE USA LLC", "FIMAT USA",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER SWAP", "SOUTH", "", 235,
			"ERCOT B E FINANCIAL", "CWATSON", nil, "", "",
			"CPT", "YES", "INTERXCHG", nil, "ERFNH", "STD ON",
			50, 111, "Y", "ER RT LMP", "NORTH_HUB_AVG",
			"HOURLY", nil, nil, nil,
			parseTime("05-05-2022"), parseTime("05-05-2022"), "NERC", "LOB2", parseTime("05-05-2022"), "LOB2",
			parseTime("05-05-2022"), "2022-05-05", "09:08:26 AM", "No",
		).AddRow(
			1388692, "PSWPS", 21600, "PURCHASE",
			parseTime("05-05-2022"), 18098, "FIMAT USA", "NEWEDGE USA LLC", "FIMAT USA",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER SWAP", "SOUTH", "", 235,
			"ERCOT B E FINANCIAL", "CWATSON", nil, "", "",
			"CPT", "YES", "INTERXCHG", nil, "ERFNH", "STD 2x16",
			135, 30, "N", "ER RT LMP", "NORTH_HUB_AVG",
			"HOURLY", nil, nil, nil,
			parseTime("01-05-2025"), parseTime("31-05-2025"), "NERC", "LOB2", parseTime("05-05-2022"), "JKING",
			parseTime("05-05-2022"), "2022-05-05", "11:05:35 AM", "No",
		))

	dealKeysArray := []int{1388598}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pswp_pswap_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerSwapDealTermModelQ := getNucPowerSwapDealTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"PSWP_PSWAP_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY"}
	mock.ExpectQuery(getNucPowerSwapDealTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1388598, 613, parseTime("05-03-2024"), parseTime("05-03-2024"),
		).AddRow(
			1388598, 614, parseTime("06-03-2024"), parseTime("06-03-2024"),
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             1388598,
					DealType:            "PSWPS",
					TotalQuantity:       800,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        18098,
					Company:             "FIMAT USA",
					CompanyLongName:     "NEWEDGE USA LLC",
					CompanyCode:         "FIMAT USA",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "POWER SWAP",
					Region:              "SOUTH",
					PrtPortfolio:        235,
					Portfolio:           "ERCOT B E FINANCIAL",
					UrTrader:            "CWATSON",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					HasBroker:           "YES",
					Broker:              "INTERXCHG",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("05-05-2022"),
					EndDate:             parseTime("05-05-2022"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     613,
							Pool1:      "ERFNH",
							Product1:   "STD ON",
							Volume:     0,
							FixedPrice: 0,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("05-03-2024"),
							EndDate:         parseTime("05-03-2024"),
							HolidaySchedule: "NERC",
						},
						{
							VolSeq:     614,
							Pool1:      "ERFNH",
							Product1:   "STD ON",
							Volume:     0,
							FixedPrice: 0,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("06-03-2024"),
							EndDate:         parseTime("06-03-2024"),
							HolidaySchedule: "NERC",
						},
					},
					CreatedBy:          "LOB2",
					CreatedAt:          parseTime("05-05-2022"),
					ModifiedBy:         "LOB2",
					ModifiedAt:         parseTime("05-05-2022"),
					ExecutionTime:      parseDateTime("2022-05-05", "09:08:26 AM"),
					ExoticFlag:         "No",
					InteraffiliateFlag: "N",
				},
				{
					DealKey:             1388692,
					DealType:            "PSWPS",
					TotalQuantity:       21600,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        18098,
					Company:             "FIMAT USA",
					CompanyLongName:     "NEWEDGE USA LLC",
					CompanyCode:         "FIMAT USA",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "POWER SWAP",
					Region:              "SOUTH",
					PrtPortfolio:        235,
					Portfolio:           "ERCOT B E FINANCIAL",
					UrTrader:            "CWATSON",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					HasBroker:           "YES",
					Broker:              "INTERXCHG",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("01-05-2025"),
					EndDate:             parseTime("31-05-2025"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     0,
							Pool1:      "ERFNH",
							Product1:   "STD 2x16",
							Volume:     135,
							FixedPrice: 30,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("01-05-2025"),
							EndDate:         parseTime("31-05-2025"),
							HolidaySchedule: "NERC",
						},
					},
					CreatedBy:          "LOB2",
					CreatedAt:          parseTime("05-05-2022"),
					ModifiedBy:         "JKING",
					ModifiedAt:         parseTime("05-05-2022"),
					ExecutionTime:      parseDateTime("2022-05-05", "11:05:35 AM"),
					ExoticFlag:         "No",
					InteraffiliateFlag: "N",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerSwapDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPowerOptionsDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"POPTION_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "IB_PRT_PORTFOLIO", "IB_PORTFOLIO", "IB_UR_TRADER",
		"TZ_TIME_ZONE", "TZ_EXERCISE_ZONE", "HAS_BROKER", "BROKER", "PPEP_PP_POOL",
		"PPEP_PEP_PRODUCT", "CTP_POINT_CODE", "SETTLE_FORMULA", "DY_BEG_DAY", "DY_END_DAY",
		"SCH_SCHEDULE", "VOLUME", "STRIKE_PRICE", "STRIKE_PRICE_TYPE", "STRIKE_FORMULA",
		"CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE", "EXECUTION_DATE",
		"EXECUTION_TIME", "EXOTIC_FLAG",
	}
	mock.ExpectQuery(getNucPowerOptionsDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			43435, "POPTS", 17600, "SALE",
			parseTime("23-05-2022"), 10322, "IBT", "INTERBOOK TRANSFER", "IBT",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER OPT", "EAST", "", 99478,
			"ERCOT_RETAIL", "GABDULLA", nil, "", "",
			"CPT", "CPT", "NO", "NA", "ERCNZ",
			"STD ON", "HB_NORTH", nil, parseTime("01-06-2022"), parseTime("30-06-2022"),
			"NERC", -50, 100, "F", "",
			"GABDULLA", "GABDULLA", parseTime("23-05-2022"), parseTime("23-05-2022"), "05/23/2022",
			"12:00:00 AM", "No",
		).AddRow(
			43441, "POPTS", 35200, "PURCHASE",
			parseTime("23-05-2022"), 10322, "IBT", "INTERBOOK TRANSFER", "IBT",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER OPT", "EAST", "", 220,
			"SOUTH OTHER HR", "GABDULLA", nil, "", "",
			"CPT", "CPT", "NO", "NA", "ERCNZ",
			"STD ON", "HB_NORTH", "[MISO RTLMP|CINERGY.HUB|HOURLY]", parseTime("01-06-2022"), parseTime("30-06-2022"),
			"NERC", 100, 100, "F", "(10.5*[GD|HOU SHP CHNL|DAILY]) + 3.55",
			"GABDULLA", "GABDULLA", parseTime("23-05-2022"), parseTime("23-05-2022"), "05/23/2022",
			"12:00:00 AM", "No",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             43435,
					DealType:            "POPTS",
					TotalQuantity:       17600,
					DnDirection:         "SALE",
					TransactionDate:     parseTime("23-05-2022"),
					CyCompanyKey:        10322,
					Company:             "IBT",
					CompanyLongName:     "INTERBOOK TRANSFER",
					CompanyCode:         "IBT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "",
					ConfirmFormat:       "POWER OPT",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        99478,
					Portfolio:           "ERCOT_RETAIL",
					UrTrader:            "GABDULLA",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					TzExerciseZone:      "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-06-2022"),
					EndDate:             parseTime("30-06-2022"),
					CreatedBy:           "GABDULLA",
					ModifiedBy:          "GABDULLA",
					CreatedAt:           parseTime("23-05-2022"),
					ModifiedAt:          parseTime("23-05-2022"),
					ExecutionTime:       parseDateTime("05/23/2022", "12:00:00 AM"),
					ExoticFlag:          "No",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							Pool1:           "ERCNZ",
							Product1:        "STD ON",
							PointCode1:      "HB_NORTH",
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("30-06-2022"),
							HolidaySchedule: "NERC",
							Volume:          -50,
							FixedPrice:      100,
							PriceType:       "F",
						},
					},
				},
				{
					DealKey:             43441,
					DealType:            "POPTS",
					TotalQuantity:       35200,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("23-05-2022"),
					CyCompanyKey:        10322,
					Company:             "IBT",
					CompanyLongName:     "INTERBOOK TRANSFER",
					CompanyCode:         "IBT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "",
					ConfirmFormat:       "POWER OPT",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        220,
					Portfolio:           "SOUTH OTHER HR",
					UrTrader:            "GABDULLA",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					TzExerciseZone:      "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-06-2022"),
					EndDate:             parseTime("30-06-2022"),
					CreatedBy:           "GABDULLA",
					ModifiedBy:          "GABDULLA",
					CreatedAt:           parseTime("23-05-2022"),
					ModifiedAt:          parseTime("23-05-2022"),
					ExecutionTime:       parseDateTime("05/23/2022", "12:00:00 AM"),
					ExoticFlag:          "No",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							Pool1:           "ERCNZ",
							Product1:        "STD ON",
							PointCode1:      "HB_NORTH",
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("30-06-2022"),
							HolidaySchedule: "NERC",
							Volume:          100,
							FixedPrice:      100,
							PriceType:       "F",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "MISO RTLMP",
									PubIndex:    "CINERGY.HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "GD",
									PubIndex:    "HOU SHP CHNL",
									Frequency:   "DAILY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerOptionsDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucCapacityDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"CAPACITY_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME",
		"COMPANYCODE", "LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "NON_STANDARD_FLAG",
		"PRICE_TYPE", "CHARGE", "VOLUME", "ENERGY_FORMULA", "PPCP_PP_POOL", "PPCP_PCP_PRODUCT",
		"CTP_POINT_CODE", "DY_BEG_DAY", "DY_END_DAY", "SCH_SCHEDULE", "CREATEDBY", "MODIFIEDBY",
		"CREATE_DATE", "MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME"}
	mock.ExpectQuery(getNucCapacityDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1104940, "CAPCTY", 972.7, "SALE",
			parseTime("05-05-2022"), 10322, "IBT", "INTERBOOK TRANSFER",
			"IBT", "SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "U-CAP", "SOUTH", "", 99690,
			"Realtime 7", "COSULLIV", "CPT", "NO", "NA", "N",
			"F", 0.1, 0, nil, "NSRS", "STD ON",
			"ERCOT", parseTime("04-05-2022"), parseTime("04-05-2022"), "NONE", "COSULLIV", "COSULLIV",
			parseTime("05-05-2022"), parseTime("05-05-2022"), "", "",
		).AddRow(
			1107318, "CAPCTY", 0.06, "PURCHASE",
			parseTime("05-05-2022"), 10296, "CAISO", "CALIFORNIA INDEPENDENT SYSTEMS OPERATION CORP DBA CALIF",
			"CALF IND S", "SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"011-KW-IS-06534", "STL CAPCTY", "WEST", "", 24,
			"POWER REAL-TIME SAN DIEGO", "PCI_ALLO", "PPT", "NO", "NA", "Y",
			"F", 0, 0, nil, "WSCC", "HOURLY",
			"CFE-IV-MX", parseTime("05-05-2022"), parseTime("05-05-2022"), "NERC", "PCI_GSMS", "PCI_GSMS",
			parseTime("18-05-2022"), parseTime("18-05-2022"), "2022-05-05", "11:36:12 AM",
		).AddRow(
			1105338, "CAPCTY", 0.24, "PURCHASE",
			parseTime("05-05-2022"), 10296, "CAISO", "CALIFORNIA INDEPENDENT SYSTEMS OPERATION CORP DBA CALIF",
			"CALF IND S", "SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"011-KW-IS-06534", "STL CAPCTY", "WEST", "", 301,
			"SETTLEMENT BRIDGE CAISO - DA", "PCI_ALLO", "PPT", "NO", "NA", "Y",
			"F", 0, 0, "([ER AS RRS|DA_RRS|HOURLY]*0.85)<CU>USD</CU><UT>MW</UT>", "WSCC", "HOURLY",
			"PGAE-APND", parseTime("05-05-2022"), parseTime("05-05-2022"), "NERC", "PCI_GSMS", "PCI_GSMS",
			parseTime("06-05-2022"), parseTime("06-05-2022"), "05/07/2022", "12:00:00 AM",
		))

	dealKeysArray := []int{1107318, 1105338}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "cpd_capacity_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucCapacityDealTermModelQ := getNucCapacityDealTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"CPD_CAPACITY_KEY", "DY_BEG_DAY", "DY_END_DAY"}
	mock.ExpectQuery(getNucCapacityDealTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1105338, parseTime("05-05-2022"), parseTime("05-05-2022"),
		).AddRow(
			1107318, parseTime("04-04-2022"), parseTime("04-04-2022"),
		).AddRow(
			1107318, parseTime("05-05-2022"), parseTime("05-05-2022"),
		))

	formulaDealKeys := []int{1105338}

	formulaDealKeysQuery, params, err := oracle.CreateInQueryInt(formulaDealKeys, []interface{}{}, "cpd_capacity_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucCapacityDealIndexModelQ := getNucCapacityDealIndexModelQuery(formulaDealKeysQuery)

	namedParams = nil
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"CPD_CAPACITY_KEY", "PUBLICATION", "PUB_INDEX", "FREQUENCY"}
	mock.ExpectQuery(getNucCapacityDealIndexModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1105338, "ER AS RRS", "DA_RRS", "HOURLY",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             1104940,
					DealType:            "CAPCTY",
					TotalQuantity:       972.7,
					DnDirection:         "SALE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10322,
					Company:             "IBT",
					CompanyLongName:     "INTERBOOK TRANSFER",
					CompanyCode:         "IBT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "",
					ConfirmFormat:       "U-CAP",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99690,
					Portfolio:           "Realtime 7",
					UrTrader:            "COSULLIV",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("04-05-2022"),
					EndDate:             parseTime("04-05-2022"),
					CreatedBy:           "COSULLIV",
					ModifiedBy:          "COSULLIV",
					CreatedAt:           parseTime("05-05-2022"),
					ModifiedAt:          parseTime("05-05-2022"),
					ExoticFlag:          "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							PriceType:       "F",
							FixedPrice:      0.1,
							Volume:          0,
							Pool1:           "NSRS",
							Product1:        "STD ON",
							PointCode1:      "ERCOT",
							BegDate:         parseTime("04-05-2022"),
							EndDate:         parseTime("04-05-2022"),
							HolidaySchedule: "NONE",
						},
					},
				},
				{
					DealKey:             1107318,
					DealType:            "CAPCTY",
					TotalQuantity:       0.06,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10296,
					Company:             "CAISO",
					CompanyLongName:     "CALIFORNIA INDEPENDENT SYSTEMS OPERATION CORP DBA CALIF",
					CompanyCode:         "CALF IND S",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "011-KW-IS-06534",
					ConfirmFormat:       "STL CAPCTY",
					Region:              "WEST",
					HsHedgeKey:          "",
					PrtPortfolio:        24,
					Portfolio:           "POWER REAL-TIME SAN DIEGO",
					UrTrader:            "PCI_ALLO",
					TzTimeZone:          "PPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("05-05-2022"),
					EndDate:             parseTime("05-05-2022"),
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("18-05-2022"),
					ModifiedAt:          parseTime("18-05-2022"),
					ExecutionTime:       parseDateTime("2022-05-05", "11:36:12 AM"),
					ExoticFlag:          "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							PriceType:       "F",
							Pool1:           "WSCC",
							Product1:        "HOURLY",
							PointCode1:      "CFE-IV-MX",
							HolidaySchedule: "NERC",
							BegDate:         parseTime("04-04-2022"),
							EndDate:         parseTime("04-04-2022"),
						},
						{
							VolSeq:          1,
							PriceType:       "F",
							Pool1:           "WSCC",
							Product1:        "HOURLY",
							PointCode1:      "CFE-IV-MX",
							HolidaySchedule: "NERC",
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
						},
					},
				},
				{
					DealKey:             1105338,
					DealType:            "CAPCTY",
					TotalQuantity:       0.24,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10296,
					Company:             "CAISO",
					CompanyLongName:     "CALIFORNIA INDEPENDENT SYSTEMS OPERATION CORP DBA CALIF",
					CompanyCode:         "CALF IND S",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "011-KW-IS-06534",
					ConfirmFormat:       "STL CAPCTY",
					Region:              "WEST",
					HsHedgeKey:          "",
					PrtPortfolio:        301,
					Portfolio:           "SETTLEMENT BRIDGE CAISO - DA",
					UrTrader:            "PCI_ALLO",
					TzTimeZone:          "PPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("05-05-2022"),
					EndDate:             parseTime("05-05-2022"),
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("06-05-2022"),
					ModifiedAt:          parseTime("06-05-2022"),
					ExecutionTime:       parseDateTime("05/07/2022", "12:00:00 AM"),
					ExoticFlag:          "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							PriceType:       "F",
							Pool1:           "WSCC",
							Product1:        "HOURLY",
							PointCode1:      "PGAE-APND",
							HolidaySchedule: "NERC",
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER AS RRS",
									PubIndex:    "DA_RRS",
									Frequency:   "HOURLY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucCapacityDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucCapacityDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPTPDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"PTP_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY",
		"LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT",
		"REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "DY_FLOW_DAY", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT",
		"PUBLICATION1", "PUB_INDEX1", "PUBLICATION2", "PUB_INDEX2", "CREATEDBY",
		"MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE"}
	mock.ExpectQuery(getNucPTPDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			273387, "PTP", "PURCHASE", parseTime("05-05-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "",
			"SOUTH", "", 99643, "ERCOT CASH NODAL", "JPROMU", "CPT",
			"NO", "NA", parseTime("06-05-2022"), "ERCRR", "HOURLY",
			"ER DA LMP", "NORTH_HUB", "ER RT LMP", "SOUTH_HUB", "PCI_GSMS",
			"PCI_GSMS", parseTime("06-05-2022"), parseTime("06-05-2022"),
		).AddRow(
			273385, "PTP", "PURCHASE", parseTime("05-05-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "",
			"SOUTH", "", 99643, "ERCOT CASH NODAL", "JPROMU", "CPT",
			"NO", "NA", parseTime("06-05-2022"), "ERCRR", "HOURLY",
			"ER DA LMP", "NORTH_HUB", "ER RT LMP", "NORTH_ZONE", "PCI_GSMS",
			"PCI_GSMS", parseTime("06-05-2022"), parseTime("06-05-2022"),
		).AddRow(
			273377, "PTP", "PURCHASE", parseTime("05-05-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "",
			"SOUTH", "", 99643, "ERCOT CASH NODAL", "JPROMU", "CPT",
			"NO", "NA", parseTime("06-05-2022"), "ERCRR", "HOURLY",
			"ER DA LMP", "HOUSTON_HUB", "ER RT LMP", "HOUSTON_ZONE", "PCI_GSMS",
			"PCI_GSMS", parseTime("06-05-2022"), parseTime("06-05-2022"),
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             273387,
					DealType:            "PTP",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99643,
					Portfolio:           "ERCOT CASH NODAL",
					UrTrader:            "JPROMU",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:   0,
							BegDate:  parseTime("06-05-2022"),
							EndDate:  parseTime("06-05-2022"),
							Pool1:    "ERCRR",
							Product1: "HOURLY",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "NORTH_HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "SOUTH_HUB",
									Frequency:   "HOURLY",
								},
							},
						},
					},
					CreatedBy:          "PCI_GSMS",
					ModifiedBy:         "PCI_GSMS",
					CreatedAt:          parseTime("06-05-2022"),
					ModifiedAt:         parseTime("06-05-2022"),
					InteraffiliateFlag: "N",
				},
				{
					DealKey:             273385,
					DealType:            "PTP",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99643,
					Portfolio:           "ERCOT CASH NODAL",
					UrTrader:            "JPROMU",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:   0,
							BegDate:  parseTime("06-05-2022"),
							EndDate:  parseTime("06-05-2022"),
							Pool1:    "ERCRR",
							Product1: "HOURLY",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "NORTH_HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_ZONE",
									Frequency:   "HOURLY",
								},
							},
						},
					},
					CreatedBy:          "PCI_GSMS",
					ModifiedBy:         "PCI_GSMS",
					CreatedAt:          parseTime("06-05-2022"),
					ModifiedAt:         parseTime("06-05-2022"),
					InteraffiliateFlag: "N",
				},
				{
					DealKey:             273377,
					DealType:            "PTP",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99643,
					Portfolio:           "ERCOT CASH NODAL",
					UrTrader:            "JPROMU",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:   0,
							BegDate:  parseTime("06-05-2022"),
							EndDate:  parseTime("06-05-2022"),
							Pool1:    "ERCRR",
							Product1: "HOURLY",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "HOUSTON_HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "HOUSTON_ZONE",
									Frequency:   "HOURLY",
								},
							},
						},
					},
					CreatedBy:          "PCI_GSMS",
					ModifiedBy:         "PCI_GSMS",
					CreatedAt:          parseTime("06-05-2022"),
					ModifiedAt:         parseTime("06-05-2022"),
					InteraffiliateFlag: "N",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPTPDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPTPDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPTPDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPTPDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPTPDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPTPDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPTPDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPTPDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucEmissionDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"EMISSION_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY",
		"LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT",
		"REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE",
		"EXECUTION_DATE", "EXECUTION_TIME"}
	mock.ExpectQuery(getNucEmissionDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			61150, "EMSSN", "SALE", parseTime("05-05-2022"),
			17038, "CSU", "THE BOARD OF TRUSTEES OF THE CALIFORNIA STATE UNIV", "CSU", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "013-KW-SU-21400", "EMISSION",
			"WEST", "CandI CRED", 99297, "WEST ENVIRON - CA RETAIL", "SGAPPY", "",
			"NO", "NA", "SGAPPY", "SGAPPY", parseTime("05-05-2022"), parseTime("05-05-2022"),
			"", "",
		).AddRow(
			61151, "EMSSN", "SALE", parseTime("05-05-2022"),
			17038, "CSU", "THE BOARD OF TRUSTEES OF THE CALIFORNIA STATE UNIV", "CSU", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "013-KW-SU-21400", "EMISSION",
			"WEST", "CandI CRED", 99297, "WEST ENVIRON - CA RETAIL", "SGAPPY", "",
			"NO", "NA", "SGAPPY", "SGAPPY", parseTime("05-05-2022"), parseTime("05-05-2022"),
			"", "",
		))

	dealKeysArray := []int{61150, 61151}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.ed_emission_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucCapacityDealTermModelQ := getNucEmissionDealListTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"ED_EMISSION_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY",
		"PRICE_TYPE", "PRICE", "VOLUME", "EPDT_EMISSION_PRODUCT", "CTP_POINT_CODE",
		"FORMULA"}
	mock.ExpectQuery(getNucCapacityDealTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			61150, 1, parseTime("01-08-2023"), parseTime("31-08-2023"),
			"F", 16.85, 10662, "CA REC", "MID-C",
			"[OPIS|CELRINC|DAILY] *0.95",
		).AddRow(
			61151, 1, parseTime("01-09-2023"), parseTime("30-09-2023"),
			"F", 16.85, 11117, "CA REC", "MID-C",
			"",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "normal run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             61150,
					DealType:            "EMSSN",
					DnDirection:         "SALE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        17038,
					Company:             "CSU",
					CompanyLongName:     "THE BOARD OF TRUSTEES OF THE CALIFORNIA STATE UNIV",
					CompanyCode:         "CSU",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-SU-21400",
					ConfirmFormat:       "EMISSION",
					Region:              "WEST",
					HsHedgeKey:          "CandI CRED",
					PrtPortfolio:        99297,
					Portfolio:           "WEST ENVIRON - CA RETAIL",
					UrTrader:            "SGAPPY",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "SGAPPY",
					ModifiedBy:          "SGAPPY",
					CreatedAt:           parseTime("05-05-2022"),
					ModifiedAt:          parseTime("05-05-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     1,
							BegDate:    parseTime("01-08-2023"),
							EndDate:    parseTime("31-08-2023"),
							PriceType:  "F",
							FixedPrice: 16.85,
							Volume:     10662,
							Product1:   "CA REC",
							PointCode1: "MID-C",
							Formula1:   "[OPIS|CELRINC|DAILY] *0.95",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "OPIS",
									PubIndex:    "CELRINC",
									Frequency:   "DAILY",
								},
							},
						},
					},
				},
				{
					DealKey:             61151,
					DealType:            "EMSSN",
					DnDirection:         "SALE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        17038,
					Company:             "CSU",
					CompanyLongName:     "THE BOARD OF TRUSTEES OF THE CALIFORNIA STATE UNIV",
					CompanyCode:         "CSU",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-SU-21400",
					ConfirmFormat:       "EMISSION",
					Region:              "WEST",
					HsHedgeKey:          "CandI CRED",
					PrtPortfolio:        99297,
					Portfolio:           "WEST ENVIRON - CA RETAIL",
					UrTrader:            "SGAPPY",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "SGAPPY",
					ModifiedBy:          "SGAPPY",
					CreatedAt:           parseTime("05-05-2022"),
					ModifiedAt:          parseTime("05-05-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     1,
							BegDate:    parseTime("01-09-2023"),
							EndDate:    parseTime("30-09-2023"),
							PriceType:  "F",
							FixedPrice: 16.85,
							Volume:     11117,
							Product1:   "CA REC",
							PointCode1: "MID-C",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucEmissionDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucEmissionDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucEmissionOptionDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"EOPTION_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY",
		"LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT", "REGION",
		"HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "ED_EMISSION_KEY", "STRIKE_PRICE", "VOLUME", "CREATEDBY",
		"MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME"}
	mock.ExpectQuery(getNucEmissionOptionDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			12706, "EMOPTS", "SALE", parseTime("27-01-2016"),
			16370, "NEWFIN", "NEWEDGE FINANCIAL INC", "NEWFIN", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "EMSSN OPT", "EAST",
			"REALIZED", 99444, "HOUSTON EMISSIONS FIN US", "DEKING", "EST",
			"YES", "TFS", 31321, 7.5, -250000, "DEKING",
			"NXTGEN", parseTime("27-01-2016"), parseTime("14-12-2016"), "01/27/2016", "10:07:59 AM",
		).AddRow(
			12711, "EMOPTS", "PURCHASE", parseTime("27-01-2016"),
			16370, "NEWFIN", "NEWEDGE FINANCIAL INC", "NEWFIN", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "EMSSN OPT", "EAST",
			"REALIZED", 99444, "HOUSTON EMISSIONS FIN US", "DEKING", "EST",
			"NO", "NA", 31326, 8, 700000, "DEKING",
			"NXTGEN", parseTime("27-01-2016"), parseTime("14-12-2016"), "01/27/2016", "10:12:59 AM",
		))

	dealKeysArray := []int{31321, 31326}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.ed_emission_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucEmissionOptionDealTermListQ := getNucEmissionOptionDealTermListQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"ED_EMISSION_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY",
		"CTP_POINT_CODE", "EPDT_EMISSION_PRODUCT"}
	mock.ExpectQuery(getNucEmissionOptionDealTermListQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			31321, 1, parseTime("01-04-2010"), parseTime("30-04-2010"), "MID-C", "CA REC",
		).AddRow(
			31326, 1, parseTime("01-04-2010"), parseTime("30-04-2010"), "MID-C", "CA REC",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             12706,
					DealType:            "EMOPTS",
					DnDirection:         "SALE",
					TransactionDate:     parseTime("27-01-2016"),
					CyCompanyKey:        16370,
					Company:             "NEWFIN",
					CompanyLongName:     "NEWEDGE FINANCIAL INC",
					CompanyCode:         "NEWFIN",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "EMSSN OPT",
					Region:              "EAST",
					HsHedgeKey:          "REALIZED",
					PrtPortfolio:        99444,
					Portfolio:           "HOUSTON EMISSIONS FIN US",
					UrTrader:            "DEKING",
					TzTimeZone:          "EST",
					HasBroker:           "YES",
					Broker:              "TFS",
					CreatedBy:           "DEKING",
					ModifiedBy:          "NXTGEN",
					CreatedAt:           parseTime("27-01-2016"),
					ModifiedAt:          parseTime("14-12-2016"),
					ExecutionTime:       parseDateTime("01/27/2016", "10:07:59 AM"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     1,
							FixedPrice: 7.5,
							Volume:     -250000,
							BegDate:    parseTime("01-04-2010"),
							EndDate:    parseTime("30-04-2010"),
							PointCode1: "MID-C",
							Product1:   "CA REC",
						},
					},
				},
				{
					DealKey:             12711,
					DealType:            "EMOPTS",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("27-01-2016"),
					CyCompanyKey:        16370,
					Company:             "NEWFIN",
					CompanyLongName:     "NEWEDGE FINANCIAL INC",
					CompanyCode:         "NEWFIN",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "EMSSN OPT",
					Region:              "EAST",
					HsHedgeKey:          "REALIZED",
					PrtPortfolio:        99444,
					Portfolio:           "HOUSTON EMISSIONS FIN US",
					UrTrader:            "DEKING",
					TzTimeZone:          "EST",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "DEKING",
					ModifiedBy:          "NXTGEN",
					CreatedAt:           parseTime("27-01-2016"),
					ModifiedAt:          parseTime("14-12-2016"),
					ExecutionTime:       parseDateTime("01/27/2016", "10:12:59 AM"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     1,
							FixedPrice: 8,
							Volume:     700000,
							BegDate:    parseTime("01-04-2010"),
							EndDate:    parseTime("30-04-2010"),
							PointCode1: "MID-C",
							Product1:   "CA REC",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucEmissionOptionDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucEmissionOptionDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucSpreadOptionsDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"SPREAD_OPTION_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY", "LEGALENTITYLONGNAME",
		"CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "IB_PRT_PORTFOLIO", "IB_PORTFOLIO", "IB_UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "DY_BEG_DAY1", "DY_END_DAY1", "SCH_SCHEDULE", "FORMULA1", "FORMULA2",
		"PPEP_PP_POOL1", "PPEP_PP_POOL2", "PPEP_PEP_PRODUCT1", "PPEP_PEP_PRODUCT2", "VOLUME",
		"STRIKE_PRICE", "CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE", "EXECUTION_DATE",
		"EXECUTION_TIME", "EXOTIC_FLAG", "POINT_CODE"}
	mock.ExpectQuery(getNucSpreadOptionsDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			77283, "SPDOPT", "PURCHASE", parseTime("25-05-2022"),
			21589, "MIDTOPO", "MIDDLETOWN POWER LLC", "MIDTOPO", "SENA", "Shell Energy North America (US), L.P.",
			10430, "013-KW-BI-32897", "DF7EXPRESO", "EAST", "FOGECA", 99896,
			"FOGECA", "GGULYASS", nil, "", "", "",
			"NO", "NA", parseTime("01-06-2022"), parseTime("30-06-2022"), "NERC", "[NE DA LMP|4000|HOURLY]", "[GD|TENN Z6 SOUTH|DAILY]",
			"XMIDX", "", "STD 7x24", "", 226,
			11, "GGULYASS", "GGULYASS", parseTime("26-05-2022"), parseTime("26-05-2022"), "05/26/2022",
			"", "Yes", "NOT APPLICABLE",
		).AddRow(
			77294, "SPDOPT", "PURCHASE", parseTime("25-05-2022"),
			21589, "MIDTOPO", "MIDDLETOWN POWER LLC", "MIDTOPO", "SENA", "Shell Energy North America (US), L.P.",
			10430, "013-KW-BI-32897", "DF7EXPRESO", "EAST", "FOGECA", 99896,
			"FOGECA", "GGULYASS", nil, "", "", "",
			"NO", "NA", parseTime("01-10-2022"), parseTime("31-10-2022"), "NERC", "([PJM DA LMP|34887979|HOURLY])", "([IF|MICHCON LEU|MONTHLY] *1.0163)",
			"XMIDX", "", "STD 7x24", "", 117,
			10.65, "GGULYASS", "GGULYASS", parseTime("26-05-2022"), parseTime("26-05-2022"), "05/26/2022",
			"", "Yes", "NOT APPLICABLE",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             77283,
					DealType:            "SPDOPT",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("25-05-2022"),
					CyCompanyKey:        21589,
					Company:             "MIDTOPO",
					CompanyLongName:     "MIDDLETOWN POWER LLC",
					CompanyCode:         "MIDTOPO",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-BI-32897",
					ConfirmFormat:       "DF7EXPRESO",
					Region:              "EAST",
					HsHedgeKey:          "FOGECA",
					PrtPortfolio:        99896,
					Portfolio:           "FOGECA",
					UrTrader:            "GGULYASS",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-06-2022"),
					EndDate:             parseTime("30-06-2022"),
					CreatedBy:           "GGULYASS",
					ModifiedBy:          "GGULYASS",
					CreatedAt:           parseTime("26-05-2022"),
					ModifiedAt:          parseTime("26-05-2022"),
					ExecutionTime:       parseDateTime("05/26/2022", ""),
					ExoticFlag:          "Yes",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							HolidaySchedule: "NERC",
							Pool1:           "XMIDX",
							Pool2:           "",
							Product1:        "STD 7x24",
							Product2:        "",
							Volume:          226,
							FixedPrice:      11,
							PointCode1:      "NOT APPLICABLE",
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("30-06-2022"),
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "NE DA LMP",
									PubIndex:    "4000",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "GD",
									PubIndex:    "TENN Z6 SOUTH",
									Frequency:   "DAILY",
								},
							},
						},
					},
				},
				{
					DealKey:             77294,
					DealType:            "SPDOPT",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("25-05-2022"),
					CyCompanyKey:        21589,
					Company:             "MIDTOPO",
					CompanyLongName:     "MIDDLETOWN POWER LLC",
					CompanyCode:         "MIDTOPO",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-BI-32897",
					ConfirmFormat:       "DF7EXPRESO",
					Region:              "EAST",
					HsHedgeKey:          "FOGECA",
					PrtPortfolio:        99896,
					Portfolio:           "FOGECA",
					UrTrader:            "GGULYASS",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-10-2022"),
					EndDate:             parseTime("31-10-2022"),
					CreatedBy:           "GGULYASS",
					ModifiedBy:          "GGULYASS",
					CreatedAt:           parseTime("26-05-2022"),
					ModifiedAt:          parseTime("26-05-2022"),
					ExecutionTime:       parseDateTime("05/26/2022", ""),
					ExoticFlag:          "Yes",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							HolidaySchedule: "NERC",
							Pool1:           "XMIDX",
							Pool2:           "",
							Product1:        "STD 7x24",
							Product2:        "",
							Volume:          117,
							FixedPrice:      10.65,
							PointCode1:      "NOT APPLICABLE",
							BegDate:         parseTime("01-10-2022"),
							EndDate:         parseTime("31-10-2022"),
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "PJM DA LMP",
									PubIndex:    "34887979",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "IF",
									PubIndex:    "MICHCON LEU",
									Frequency:   "MONTHLY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucSpreadOptionsDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucSpreadOptionsDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucHeatRateSwapsDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"HRSWPS_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "OPTION_KEY",
		"PPEP_PP_POOL", "PPEP_PEP_PRODUCT", "PIF_PI_PB_PUBLICATION1", "PIF_PI_PUB_INDEX1",
		"PIF_PI_PB_PUBLICATION2", "PIF_PI_PUB_INDEX2", "PIF_FRQ_FREQUENCY2", "DY_BEG_DAY",
		"DY_END_DAY", "SCH_SCHEDULE", "VOLUME1", "CREATEDBY", "MODIFIEDBY", "CREATE_DATE",
		"MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME"}
	mock.ExpectQuery(getNucHeatRateSwapsDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			16495, "HRSWPS", 175200, "SALE",
			parseTime("01-06-2022"), 10105, "O&R", "ORANGE AND ROCKLAND UTILITIES INC", "ORNGE&RCK",
			"STRM", "SHELL TRADING RISK MANAGEMENT LLC", 18726, "N",
			"033-RM-FI-10415", "HR SWAP", "EAST", "", 99482,
			"POWER_RM_CONSUMERS", "DMOLIN", "", "NO", "NA", nil,
			"NYCAG", "STD 7x24", "NY DA LMP", "61758",
			"NYMEX", "NG", "MONTHLY", parseTime("23-01-2023"),
			parseTime("31-12-2023"), "NERC", -20, "DMOLIN", "DMOLIN", parseTime("01-06-2022"),
			parseTime("01-06-2022"), "06/01/2022", "09:10:00 AM",
		).AddRow(
			16496, "HRSWPS", 175200, "PURCHASE",
			parseTime("01-06-2022"), 10430, "SENA", "Shell Energy North America (US), L.P.", "SENA",
			"STRM", "SHELL TRADING RISK MANAGEMENT LLC", 18726, "Y",
			"033-RM-FI-20570", "HR SWAP", "EAST", "", 99482,
			"POWER_RM_CONSUMERS", "DMOLIN", "", "NO", "NA", nil,
			"NYCAG", "STD 7x24", "NY DA LMP", "61758",
			"NYMEX", "NG", "MONTHLY", parseTime("23-01-2023"),
			parseTime("31-12-2023"), "NERC", 20, "DMOLIN", "DMOLIN", parseTime("01-06-2022"),
			parseTime("01-06-2022"), "06/01/2022", "09:10:00 AM",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "test regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             16495,
					DealType:            "HRSWPS",
					TotalQuantity:       175200,
					DnDirection:         "SALE",
					TransactionDate:     parseTime("01-06-2022"),
					CyCompanyKey:        10105,
					Company:             "O&R",
					CompanyLongName:     "ORANGE AND ROCKLAND UTILITIES INC",
					CompanyCode:         "ORNGE&RCK",
					LegalEntity:         "STRM",
					LegalEntityLongName: "SHELL TRADING RISK MANAGEMENT LLC",
					CyLegalEntityKey:    18726,
					InteraffiliateFlag:  "N",
					Contract:            "033-RM-FI-10415",
					ConfirmFormat:       "HR SWAP",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        99482,
					Portfolio:           "POWER_RM_CONSUMERS",
					UrTrader:            "DMOLIN",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("23-01-2023"),
					EndDate:             parseTime("31-12-2023"),
					CreatedBy:           "DMOLIN",
					ModifiedBy:          "DMOLIN",
					CreatedAt:           parseTime("01-06-2022"),
					ModifiedAt:          parseTime("01-06-2022"),
					ExecutionTime:       parseDateTime("06/01/2022", "09:10:00 AM"),
					ExoticFlag:          "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							Pool1:           "NYCAG",
							Product1:        "STD 7x24",
							HolidaySchedule: "NERC",
							Volume:          -20,
							BegDate:         parseTime("23-01-2023"),
							EndDate:         parseTime("31-12-2023"),
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "NY DA LMP",
									PubIndex:    "61758",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "NYMEX",
									PubIndex:    "NG",
									Frequency:   "MONTHLY",
								},
								{
									Publication: "NYMEX",
									PubIndex:    "NG",
									Frequency:   "MONTHLY",
								},
							},
						},
					},
				},
				{
					DealKey:             16496,
					DealType:            "HRSWPS",
					TotalQuantity:       175200,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("01-06-2022"),
					CyCompanyKey:        10430,
					Company:             "SENA",
					CompanyLongName:     "Shell Energy North America (US), L.P.",
					CompanyCode:         "SENA",
					LegalEntity:         "STRM",
					LegalEntityLongName: "SHELL TRADING RISK MANAGEMENT LLC",
					CyLegalEntityKey:    18726,
					InteraffiliateFlag:  "Y",
					Contract:            "033-RM-FI-20570",
					ConfirmFormat:       "HR SWAP",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        99482,
					Portfolio:           "POWER_RM_CONSUMERS",
					UrTrader:            "DMOLIN",
					TzTimeZone:          "",
					HasBroker:           "NO",
					Broker:              "NA",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("23-01-2023"),
					EndDate:             parseTime("31-12-2023"),
					CreatedBy:           "DMOLIN",
					ModifiedBy:          "DMOLIN",
					CreatedAt:           parseTime("01-06-2022"),
					ModifiedAt:          parseTime("01-06-2022"),
					ExecutionTime:       parseDateTime("06/01/2022", "09:10:00 AM"),
					ExoticFlag:          "NA",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							Pool1:           "NYCAG",
							Product1:        "STD 7x24",
							HolidaySchedule: "NERC",
							Volume:          20,
							BegDate:         parseTime("23-01-2023"),
							EndDate:         parseTime("31-12-2023"),
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "NY DA LMP",
									PubIndex:    "61758",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "NYMEX",
									PubIndex:    "NG",
									Frequency:   "MONTHLY",
								},
								{
									Publication: "NYMEX",
									PubIndex:    "NG",
									Frequency:   "MONTHLY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucHeatRateSwapsDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucHeatRateSwapsDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucTCCFTRSDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"DEAL_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY",
		"LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT",
		"REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "DY_BEG_DAY", "DY_END_DAY", "SCH_SCHEDULE", "VOLUME",
		"FIXED_PRICE", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT", "PI_PB_PUBLICATION", "FRQ_FREQUENCY",
		"POI_PI_PUB_INDEX", "POW_PI_PUB_INDEX", "CREATEDBY", "MODIFIEDBY", "CREATE_DATE",
		"MODIFY_DATE"}
	mock.ExpectQuery(getNucTCCFTRSDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("strDealType", "FTRSWP"), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			262325, "FTRSWP", "PURCHASE", parseTime("02-06-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "FTRSWAP",
			"SOUTH", "", 99860, "SP ERCOT CRR", "SPARK", "CPT",
			"NO", "NA", parseTime("01-03-2023"), parseTime("31-03-2023"), "NERC", 100,
			3.0205, "ERCRR", "STD ON", "ER DA LMP", "HOURLY",
			"SOUTH_HUB", "LZ_LCRA", "PCI_GSMS", "PCI_GSMS", parseTime("02-06-2022"),
			parseTime("02-06-2022"),
		).AddRow(
			262147, "FTRSWP", "PURCHASE", parseTime("02-06-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "", "FTRSWAP",
			"SOUTH", "", 99860, "SP ERCOT CRR", "SPARK", "CPT",
			"NO", "NA", parseTime("01-01-2023"), parseTime("31-01-2023"), "NERC", 25,
			4.10714, "ERCRR", "STD ON", "ER DA LMP", "HOURLY",
			"SOUTH_HUB", "NORTH_HUB", "PCI_GSMS", "PCI_GSMS", parseTime("02-06-2022"),
			parseTime("02-06-2022"),
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
		strDealType string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
				strDealType: "FTRSWP",
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             262325,
					DealType:            "FTRSWP",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "FTRSWAP",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99860,
					Portfolio:           "SP ERCOT CRR",
					UrTrader:            "SPARK",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("01-03-2023"),
							EndDate:         parseTime("31-03-2023"),
							HolidaySchedule: "NERC",
							Volume:          100,
							FixedPrice:      3.0205,
							Pool1:           "ERCRR",
							Product1:        "STD ON",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "SOUTH_HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "LZ_LCRA",
									Frequency:   "HOURLY",
								},
							},
						},
					},
				},
				{
					DealKey:             262147,
					DealType:            "FTRSWP",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "FTRSWAP",
					Region:              "SOUTH",
					HsHedgeKey:          "",
					PrtPortfolio:        99860,
					Portfolio:           "SP ERCOT CRR",
					UrTrader:            "SPARK",
					TzTimeZone:          "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("01-01-2023"),
							EndDate:         parseTime("31-01-2023"),
							HolidaySchedule: "NERC",
							Volume:          25,
							FixedPrice:      4.10714,
							Pool1:           "ERCRR",
							Product1:        "STD ON",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "SOUTH_HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER DA LMP",
									PubIndex:    "NORTH_HUB",
									Frequency:   "HOURLY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucTCCFTRSDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate, tt.args.strDealType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucTCCFTRSDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucTransmissionDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"TRANS_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY",
		"LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT",
		"REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE",
		"HAS_BROKER", "BROKER", "CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE",
		"EXECUTION_TIME"}

	mock.ExpectQuery(getNucTransmissionDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			323931, "TRANS", "PURCHASE", parseTime("02-06-2022"),
			10133, "SMUD", "SACRAMENTO MUNICIPAL UTILITY DIST", "SMUD", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "011-KW-BI-03761", "TRANS",
			"WEST", "N", 288, "SD - TRANS", "LHILER", "PPT",
			"NO", "NA", "LHILER", "LHILER", parseTime("02-06-2022"), parseTime("02-06-2022"),
			"01/01/1900",
		).AddRow(
			323934, "TRANS", "SALE", parseTime("02-06-2022"),
			10363, "PEXC", "POWEREX CORP", "POWEREX", "SENA",
			"Shell Energy North America (US), L.P.", 10430, "011-KW-BI-03761", "TRANS",
			"WEST", "N", 288, "SD - TRANS", "LHILER", "PPT",
			"NO", "NA", "LHILER", "LHILER", parseTime("02-06-2022"), parseTime("02-06-2022"),
			"01/01/1900",
		))

	dealKeysArray := []int{323931, 323934}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.td_trans_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucTransmissionDealTermListQ := getNucTransmissionDealTermListQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"TD_TRANS_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY", "VOLUME",
		"PPEP_PEP_PRODUCT", "PPEP_PP_FM_POOL", "CTP_FM_POINT_CODE", "PPEP_PP_TO_POOL",
		"CTP_TO_POINT_CODE", "SCH_SCHEDULE"}

	mock.ExpectQuery(getNucTransmissionDealTermListQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			323931, 0, parseTime("01-06-2022"), parseTime("01-06-2022"), 500,
			"HOURLY", "MID-C", "MID-C", "MID-C",
			"MID-C", "NERC",
		).AddRow(
			323931, 1, parseTime("01-06-2022"), parseTime("01-06-2022"), 414,
			"HOURLY", "MID-C", "MID-C", "MID-C",
			"MID-C", "NERC",
		).AddRow(
			323934, 0, parseTime("01-06-2022"), parseTime("01-06-2022"), 151,
			"HOURLY", "MID-C", "MID-C", "MID-C",
			"MID-C", "NERC",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             323931,
					DealType:            "TRANS",
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10133,
					Company:             "SMUD",
					CompanyLongName:     "SACRAMENTO MUNICIPAL UTILITY DIST",
					CompanyCode:         "SMUD",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "011-KW-BI-03761",
					ConfirmFormat:       "TRANS",
					Region:              "WEST",
					HsHedgeKey:          "N",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "LHILER",
					TzTimeZone:          "PPT",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "LHILER",
					ModifiedBy:          "LHILER",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					ExecutionTime:       parseTime("01-01-1900"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("01-06-2022"),
							Volume:          500,
							Product1:        "HOURLY",
							Pool1:           "MID-C",
							PointCode1:      "MID-C",
							Pool2:           "MID-C",
							PointCode2:      "MID-C",
							HolidaySchedule: "NERC",
							PriceType:       "F",
						},
						{
							VolSeq:          1,
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("01-06-2022"),
							Volume:          414,
							Product1:        "HOURLY",
							Pool1:           "MID-C",
							PointCode1:      "MID-C",
							Pool2:           "MID-C",
							PointCode2:      "MID-C",
							HolidaySchedule: "NERC",
							PriceType:       "F",
						},
					},
				},
				{
					DealKey:             323934,
					DealType:            "TRANS",
					DnDirection:         "SALE",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10363,
					Company:             "PEXC",
					CompanyLongName:     "POWEREX CORP",
					CompanyCode:         "POWEREX",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "011-KW-BI-03761",
					ConfirmFormat:       "TRANS",
					Region:              "WEST",
					HsHedgeKey:          "N",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "LHILER",
					TzTimeZone:          "PPT",
					HasBroker:           "NO",
					Broker:              "NA",
					CreatedBy:           "LHILER",
					ModifiedBy:          "LHILER",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					ExoticFlag:          "NA",
					InteraffiliateFlag:  "N",
					ExecutionTime:       parseTime("01-01-1900"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("01-06-2022"),
							Volume:          151,
							Product1:        "HOURLY",
							Pool1:           "MID-C",
							PointCode1:      "MID-C",
							Pool2:           "MID-C",
							PointCode2:      "MID-C",
							HolidaySchedule: "NERC",
							PriceType:       "F",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucTransmissionDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucTransmissionDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucMiscChargeDealList(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	columns := []string{"MISC_CHARGE_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE", "LEGALENTITY", "LEGALENTITYLONGNAME",
		"CYLEGALENTITYKEY", "CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY",
		"PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER", "TZ_TIME_ZONE", "HAS_BROKER", "BROKER",
		"CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE"}

	mock.ExpectQuery(getNucMiscChargeDealListQuery).WithArgs(sql.Named("tradeDate", now), sql.Named("lastRunTime", now)).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			2854193, "MISC", "Payable", parseTime("02-06-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA", "Shell Energy North America (US), L.P.",
			10430, "013-KW-SA-20161", "", "", "",
			255, "CEM ODD LOT", "PCI_ALLO", "", "", "",
			"PCI_GSMS", "PCI_GSMS", parseTime("02-06-2022"), parseTime("02-06-2022"),
		).AddRow(
			2854183, "MISC", "Receivable", parseTime("02-06-2022"),
			10171, "ERCOT", "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC", "ERCOT", "SENA", "Shell Energy North America (US), L.P.",
			10430, "013-KW-SA-20161", "", "", "",
			255, "CEM ODD LOT", "PCI_ALLO", "", "", "",
			"PCI_GSMS", "PCI_GSMS", parseTime("02-06-2022"), parseTime("02-06-2022"),
		))

	dealKeysArray := []int{2854193, 2854183}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.mc_misc_charge_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucMiscChargeDealTermListQ := getNucMiscChargeDealTermListQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"MC_MISC_CHARGE_KEY", "MISC_VOL_SEQ", "DY_BEG_DAY", "DY_END_DAY", "INT_VOLUME"}

	mock.ExpectQuery(getNucMiscChargeDealTermListQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			2854183, 0, parseTime("04-04-2022"), parseTime("04-04-2022"), 0,
		).AddRow(
			2854193, 0, parseTime("25-05-2022"), parseTime("25-05-2022"), 0,
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx         context.Context
		lastRunTime time.Time
		tradeDate   time.Time
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:         context.TODO(),
				lastRunTime: now,
				tradeDate:   now,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             2854193,
					DealType:            "MISC",
					DnDirection:         "Payable",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-SA-20161",
					ConfirmFormat:       "",
					Region:              "",
					HsHedgeKey:          "",
					PrtPortfolio:        255,
					Portfolio:           "CEM ODD LOT",
					UrTrader:            "PCI_ALLO",
					TzTimeZone:          "",
					HasBroker:           "",
					Broker:              "",
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:  0,
							BegDate: parseTime("25-05-2022"),
							EndDate: parseTime("25-05-2022"),
							Volume:  0,
						},
					},
				},
				{
					DealKey:             2854183,
					DealType:            "MISC",
					DnDirection:         "Receivable",
					TransactionDate:     parseTime("02-06-2022"),
					CyCompanyKey:        10171,
					Company:             "ERCOT",
					CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
					CompanyCode:         "ERCOT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "013-KW-SA-20161",
					ConfirmFormat:       "",
					Region:              "",
					HsHedgeKey:          "",
					PrtPortfolio:        255,
					Portfolio:           "CEM ODD LOT",
					UrTrader:            "PCI_ALLO",
					TzTimeZone:          "",
					HasBroker:           "",
					Broker:              "",
					CreatedBy:           "PCI_GSMS",
					ModifiedBy:          "PCI_GSMS",
					CreatedAt:           parseTime("02-06-2022"),
					ModifiedAt:          parseTime("02-06-2022"),
					InteraffiliateFlag:  "N",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:  0,
							BegDate: parseTime("04-04-2022"),
							EndDate: parseTime("04-04-2022"),
							Volume:  0,
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucMiscChargeDealList(tt.args.ctx, tt.args.lastRunTime, tt.args.tradeDate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucMiscChargeDealList() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetLastExtractionRun(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	machineLearningDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	columns := []string{"ExtractionRunId", "TransactionDate", "DealType", "TimeParameter", "CreatedAt"}
	mock.ExpectQuery(getLastExtractionRunQuery).WithArgs(sql.Named("transactionDate", now), sql.Named("dealType", "POWER")).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			461,
			parseTime("01-12-2022"),
			"POWER",
			parseTimeLayout("2006-01-02 15:04:05.000", "2022-01-12 00:00:00.000"),
			parseTimeLayout("2006-01-02 15:04:05.000", "2022-01-13 17:57:14.850"),
		))

	mock.ExpectQuery(getLastExtractionRunQuery).WithArgs(sql.Named("transactionDate", parseTime("12-12-2030")), sql.Named("dealType", "ALL")).
		WillReturnRows(sqlmock.NewRows(columns))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx       context.Context
		tradeDate time.Time
		dealType  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *nucleus.NucleusTradeExtractionRunModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx:       context.TODO(),
				tradeDate: now,
				dealType:  "POWER",
			},
			want: &nucleus.NucleusTradeExtractionRunModel{
				ExtractionRunId: 461,
				TransactionDate: parseTime("01-12-2022"),
				DealType:        "POWER",
				TimeParameter:   parseTimeLayout("2006-01-02 15:04:05.000", "2022-01-12 00:00:00.000"),
				CreatedAt:       parseTimeLayout("2006-01-02 15:04:05.000", "2022-01-13 17:57:14.850"),
			},
			wantErr: false,
		},
		{
			name: "no rows in result set",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx:       context.TODO(),
				tradeDate: parseTime("12-12-2030"),
				dealType:  "ALL",
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetLastExtractionRun(tt.args.ctx, tt.args.tradeDate, tt.args.dealType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetLastExtractionRun() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetLastExtractionRun() = %v, want %v", got, tt.want)
			}
		})
	}
}

type processedTradeExample struct {
	*common.ResultModelBasePayload
	Company  string
	Trader   string
	Source   string
	Location string
	Broker   string
}

type mockNucleusProcessedTradeConverter struct{}

func (converter *mockNucleusProcessedTradeConverter) ConvertValue(raw interface{}) (driver.Value, error) {
	switch inner := raw.(type) {
	case string:
		return raw.(string), nil
	case mssql.TVP:
		if inner.TypeName != "NucleusProcessedTradeType" {
			return nil, fmt.Errorf("invalid type")
		}
		return "PASSED", nil
	}

	return nil, fmt.Errorf("invalid type")
}

func TestNucleusTradeRepository_ProcessTrades(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	machineLearningDb, mock, err := sqlmock.New(sqlmock.ValueConverterOption(&mockNucleusProcessedTradeConverter{}))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	anomalyMessages := make(map[int][]common.IModelBasePayload)
	anomalyMessages[14695753] = []common.IModelBasePayload{
		processedTradeExample{
			Company:  "NGX G_LGL",
			Trader:   "sharper",
			Source:   "ECOM_ICE|ICE_FUT",
			Location: "47288|HUNTINGDON",
			Broker:   "INTERXCHG_BKR",
			ResultModelBasePayload: &common.ResultModelBasePayload{
				DealKey:     14695753,
				DealType:    "COMM-PHYS",
				ModelName:   "ModelName",
				Message:     "Message",
				ScoredLabel: "NO",
			},
		},
		processedTradeExample{
			Company:  "NGX G_LGL",
			Trader:   "sharper",
			Source:   "ECOM_ICE|ICE_FUT",
			Location: "47288|HUNTINGDON",
			Broker:   "INTERXCHG_BKR",
			ResultModelBasePayload: &common.ResultModelBasePayload{
				DealKey:     14695753,
				DealType:    "COMM-PHYS",
				ModelName:   "ModelName",
				Message:     "Message",
				ScoredLabel: "YES",
			},
		},
	}

	var nucleusProcessedTradeTypeData []*nucleusProcessedTradeType

	trades := []*nucleus.NucleusTradeHeaderModel{
		{
			DealKey:             3995652,
			DealType:            "PWRNSD",
			TotalQuantity:       0,
			TransactionDate:     parseTime("02-05-2022"),
			DnDirection:         "PURCHASE",
			CyCompanyKey:        10171,
			CompanyCode:         "ERCOT",
			Company:             "ERCOT",
			CompanyLongName:     "ELECTRIC RELIABILITY COUNCIL OF TEXAS INC",
			PrtPortfolio:        99817,
			Portfolio:           "TEJAS GEN FRIENDSWOOD",
			UrTrader:            "BSILVA",
			CyBrokerKey:         0,
			Broker:              "NA",
			TzTimeZone:          "CPT",
			HasBroker:           "NO",
			ConfirmFormat:       "STL PWRNSD",
			CyLegalEntityKey:    21014,
			LegalEntity:         "TEJ PWR GN",
			LegalEntityLongName: "TEJAS POWER GENERATION LLC",
			Region:              "SOUTH",
			Terms: []*nucleus.NucleusTradeTermModel{
				{
					VolSeq:          0,
					BegDate:         parseTime("03-05-2022"),
					EndDate:         parseTime("03-05-2022"),
					Pool1:           "ERHOU",
					Product1:        "HOURLY",
					PointCode1:      "HB_HOUSTON",
					HolidaySchedule: "NONE",
					Formula1:        "([ER RT LMP|HOUSTON_HUB_AVG|HOURLY])<CU>USD</CU><UT>MW</UT>",
					Indexes1: []*nucleus.NucleusTradeIndexModel{
						{
							Publication: "ER RT LMP",
							PubIndex:    "HOUSTON_HUB_AVG",
							Frequency:   "HOURLY",
						},
					},
					PriceType: "I",
				},
			},
			AnomalyTestResult:  "",
			CreatedAt:          parseTime("02-05-2022"),
			ModifiedAt:         parseDateTime("2022-05-18", "07:53:32 AM"),
			CreatedBy:          "CAGUILAR",
			ModifiedBy:         "JCHOI",
			ExoticFlag:         "NA",
			InteraffiliateFlag: "N",
		},
	}

	for _, trade := range trades {
		var anomalyMsg, modelParams string

		if resultList, ok := anomalyMessages[trade.DealKey]; ok {
			for _, result := range resultList {
				if result.GetScoredLabel() == "NO" {
					if anomalyMsg == "" {
						anomalyMsg = result.GetModelName() + " " + result.GetMessage()
					} else {
						anomalyMsg = anomalyMsg + ";" + result.GetModelName() + " " + result.GetMessage()
					}

					vParams, err := json.Marshal(result)
					if err != nil {
						t.Fatalf("an error '%v' was not expected when marshalling json", err)
					}

					if modelParams == "" {
						modelParams = string(vParams)
					} else {
						modelParams = modelParams + ";" + string(vParams)
					}
				}
			}
		}

		marshalledTrade, err := json.Marshal(trade)
		if err != nil {
			t.Fatalf("an error '%v' was not expected when marshalling json", err)
		}

		nucleusProcessedTrade := &nucleusProcessedTradeType{
			TradeId:             int64(trade.DealKey),
			DealType:            trade.DealType,
			PortfolioId:         int64(trade.PrtPortfolio),
			TransactionDate:     trade.TransactionDate,
			TradeDetail:         string(marshalledTrade),
			AnomalyDetectedFlag: false,
			AnomalyTestResult: sql.NullString{
				Valid: false,
			},
			ModelParameters: sql.NullString{
				Valid: false,
			},
		}

		if anomalyMsg != "" {
			nucleusProcessedTrade.AnomalyDetectedFlag = true
			nucleusProcessedTrade.AnomalyTestResult = sql.NullString{
				String: anomalyMsg,
				Valid:  true,
			}
		}

		if modelParams != "" {
			nucleusProcessedTrade.AnomalyTestResult = sql.NullString{
				String: modelParams,
				Valid:  true,
			}
		}

		nucleusProcessedTradeTypeData = append(nucleusProcessedTradeTypeData, nucleusProcessedTrade)
	}

	tvpType := mssql.TVP{
		TypeName: "NucleusProcessedTradeType",
		Value:    nucleusProcessedTradeTypeData,
	}

	mock.ExpectExec(execProcessTradesQuery).WithArgs(sql.Named("TVP", tvpType)).WillReturnResult(driver.ResultNoRows)

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx             context.Context
		trades          []*nucleus.NucleusTradeHeaderModel
		anomalyMessages map[int][]common.IModelBasePayload
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "normal run",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx:             context.TODO(),
				trades:          trades,
				anomalyMessages: anomalyMessages,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			if err := repo.ProcessTrades(tt.args.ctx, tt.args.trades, tt.args.anomalyMessages); (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.ProcessTrades() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNucleusTradeRepository_InsertExtractionRun(t *testing.T) {
	now := time.Now()
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	machineLearningDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	mock.ExpectExec(insertExtractionRunQuery).WithArgs(sql.Named("transactionDate", now), sql.Named("dealType", "FTRSWP"), sql.Named("timeParameter", now)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx       context.Context
		tradeDate time.Time
		lastRun   time.Time
		dealType  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test regular run",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx:       context.TODO(),
				tradeDate: now,
				lastRun:   now,
				dealType:  "FTRSWP",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			if err := repo.InsertExtractionRun(tt.args.ctx, tt.args.tradeDate, tt.args.lastRun, tt.args.dealType); (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.InsertExtractionRun() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNucleusTradeRepository_GetPortfolioRiskMappingList(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	machineLearningDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	machineLearningDbEmpty, mockEmpty, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	machineLearningDbError, mockError, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	columns := []string{"SourceSystem", "Portfolio", "LegalEntity"}

	mock.ExpectQuery(getPortfolioRiskMappingListQuery).WillReturnRows(sqlmock.NewRows(columns).AddRow(
		"NUCLEUS", "LOCKPORT_TOLL", nil,
	).AddRow(
		"NUCLEUS", "COMMITMENT FEE INTEREST", "STRM",
	))

	mockEmpty.ExpectQuery(getPortfolioRiskMappingListQuery).WillReturnRows(sqlmock.NewRows(columns))

	mockError.ExpectQuery(getPortfolioRiskMappingListQuery).WillReturnError(fmt.Errorf("error"))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.PortfolioRiskMappingModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want: []*nucleus.PortfolioRiskMappingModel{
				{
					SourceSystem: "NUCLEUS",
					Portfolio:    "LOCKPORT_TOLL",
					LegalEntity:  "",
				},
				{
					SourceSystem: "NUCLEUS",
					Portfolio:    "COMMITMENT FEE INTEREST",
					LegalEntity:  "STRM",
				},
			},
			wantErr: false,
		},
		{
			name: "no rows in result set",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDbEmpty,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "error in result set",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDbError,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetPortfolioRiskMappingList(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetPortfolioRiskMappingList() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetPortfolioRiskMappingList() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNucleusTradeRepository_GetLarBaselist(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	machineLearningDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	machineLearningDbEmpty, mockEmpty, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	machineLearningDbError, mockError, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer machineLearningDb.Close()

	columns := []string{"ShortName", "CounterpartyLongName", "ParentCompany", "Product",
		"SourceSystem", "DealType", "NettingAgreement", "AgreementTypePerCSA", "OurThreshold",
		"CounterpartyThreshold", "BuyTenor", "SellTenor", "GrossExposure", "Collateral",
		"NetPosition", "LimitValue", "LimitCurrency", "LimitAvailability", "ExposureLimit",
		"ExpirationDate", "MarketType", "IndustryCode", "SPRating", "MoodyRating",
		"FinalInternalRating", "FinalRating", "Equifax", "AmendedBy", "EffectiveDate",
		"ReviewDate", "DoddFrankClassification", "ReportCreatedDate", "Boost", "tradingEntity",
		"LegalEntity", "Agmt", "CSA", "Tenor", "Limit", "ReportingDate", "CreatedAt"}

	mock.ExpectQuery(getLarBaselistQuery).WillReturnRows(sqlmock.NewRows(columns).AddRow(
		"ONT POWER", "ONTARIO POWER GENERATION INC", "ONTARIO POWER GENERATION INC", "SENA-Power",
		"NUCLEUS", "CAPCTY", "011-KW-BI-03761", "WSPP with Netting", 0,
		10, "Halt Trading -KYC", "Halt Trading -KYC", 0.00, 0,
		0, 0, "USD", "HALT TRADING - KYC", "",
		parseTimeLayout("2006-01-02 15:04:05.000", "2099-12-31 00:00:00.000"), "Wholesale", "55101010 - Electric Utilities", "BBB+", "Not Rated",
		"BBB+", "BBB+", " ", "Icko Moster", parseTimeLayout("2006-01-02 15:04:05.000", "2020-10-15 00:00:00.000"),
		parseTimeLayout("2006-01-02 15:04:05.000", "2099-12-31 00:00:00.000"), "No Classification", parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:30:15.143"), "FALSE", "SHELL ENERGY NORTH AMERICA (US) LP",
		"Shell Energy North America (US), L.P.", "X", "X", "X", "X", parseTime("20-10-2021"), parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:22:25.910"),
	).AddRow(
		"OPSI", "OCCIDENTAL POWER SERVICES INC", "OCCIDENTAL PETROLEUM CORPORATION", "SENA-Power",
		"NUCLEUS", "POWER", "011-KW-BI-03761", "WSPP with Netting", 10,
		10, "CALL CREDIT", "CALL CREDIT", 0.00, 0,
		0, 0, "USD", "CALL CREDIT", "HIGH",
		parseTimeLayout("2006-01-02 15:04:05.000", "2022-09-30 00:00:00.000"), "Wholesale", "40203020 - Investment Banking and Brokerage", "Not Rated", "Not Rated",
		"CCC+", "CCC+", " ", "Sarah Marissa Jynnyl Vista", parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-06 00:00:00.000"),
		parseTimeLayout("2006-01-02 15:04:05.000", "2022-08-30 00:00:00.000"), "No Classification", parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:30:15.143"), "FALSE", "SHELL ENERGY NORTH AMERICA (US) LP",
		"Shell Energy North America (US), L.P.", "X", "X", "X", "X", parseTime("20-10-2021"), parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:22:25.910"),
	))

	mockEmpty.ExpectQuery(getLarBaselistQuery).WillReturnRows(sqlmock.NewRows(columns))

	mockError.ExpectQuery(getLarBaselistQuery).WillReturnError(fmt.Errorf("error"))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*common.LarBaseModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDb,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want: []*common.LarBaseModel{
				{
					ShortName:               "ONT POWER",
					CounterpartyLongName:    "ONTARIO POWER GENERATION INC",
					ParentCompany:           "ONTARIO POWER GENERATION INC",
					Product:                 "SENA-Power",
					SourceSystem:            "NUCLEUS",
					DealType:                "CAPCTY",
					NettingAgreement:        "011-KW-BI-03761",
					AgreementTypePerCSA:     "WSPP with Netting",
					OurThreshold:            0,
					CounterpartyThreshold:   10,
					BuyTenor:                "Halt Trading -KYC",
					SellTenor:               "Halt Trading -KYC",
					GrossExposure:           0.00,
					Collateral:              0,
					NetPosition:             0,
					LimitValue:              0,
					LimitCurrency:           "USD",
					LimitAvailability:       "HALT TRADING - KYC",
					ExposureLimit:           "",
					ExpirationDate:          parseTimeLayout("2006-01-02 15:04:05.000", "2099-12-31 00:00:00.000"),
					MarketType:              "Wholesale",
					IndustryCode:            "55101010 - Electric Utilities",
					SPRating:                "BBB+",
					MoodyRating:             "Not Rated",
					FinalInternalRating:     "BBB+",
					FinalRating:             "BBB+",
					Equifax:                 " ",
					AmendedBy:               "Icko Moster",
					EffectiveDate:           parseTimeLayout("2006-01-02 15:04:05.000", "2020-10-15 00:00:00.000"),
					ReviewDate:              parseTimeLayout("2006-01-02 15:04:05.000", "2099-12-31 00:00:00.000"),
					DoddFrankClassification: "No Classification",
					ReportCreatedDate:       parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:30:15.143"),
					Boost:                   "FALSE",
					TradingEntity:           "SHELL ENERGY NORTH AMERICA (US) LP",
					LegalEntity:             "Shell Energy North America (US), L.P.",
					Agmt:                    "X",
					CSA:                     "X",
					Tenor:                   "X",
					Limit:                   "X",
					ReportingDate:           parseTime("20-10-2021"),
					CreatedAt:               parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:22:25.910"),
				},
				{
					ShortName:               "OPSI",
					CounterpartyLongName:    "OCCIDENTAL POWER SERVICES INC",
					ParentCompany:           "OCCIDENTAL PETROLEUM CORPORATION",
					Product:                 "SENA-Power",
					SourceSystem:            "NUCLEUS",
					DealType:                "POWER",
					NettingAgreement:        "011-KW-BI-03761",
					AgreementTypePerCSA:     "WSPP with Netting",
					OurThreshold:            10,
					CounterpartyThreshold:   10,
					BuyTenor:                "CALL CREDIT",
					SellTenor:               "CALL CREDIT",
					GrossExposure:           0.00,
					Collateral:              0,
					NetPosition:             0,
					LimitValue:              0,
					LimitCurrency:           "USD",
					LimitAvailability:       "CALL CREDIT",
					ExposureLimit:           "HIGH",
					ExpirationDate:          parseTimeLayout("2006-01-02 15:04:05.000", "2022-09-30 00:00:00.000"),
					MarketType:              "Wholesale",
					IndustryCode:            "40203020 - Investment Banking and Brokerage",
					SPRating:                "Not Rated",
					MoodyRating:             "Not Rated",
					FinalInternalRating:     "CCC+",
					FinalRating:             "CCC+",
					Equifax:                 " ",
					AmendedBy:               "Sarah Marissa Jynnyl Vista",
					EffectiveDate:           parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-06 00:00:00.000"),
					ReviewDate:              parseTimeLayout("2006-01-02 15:04:05.000", "2022-08-30 00:00:00.000"),
					DoddFrankClassification: "No Classification",
					ReportCreatedDate:       parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:30:15.143"),
					Boost:                   "FALSE",
					TradingEntity:           "SHELL ENERGY NORTH AMERICA (US) LP",
					LegalEntity:             "Shell Energy North America (US), L.P.",
					Agmt:                    "X",
					CSA:                     "X",
					Tenor:                   "X",
					Limit:                   "X",
					ReportingDate:           parseTime("20-10-2021"),
					CreatedAt:               parseTimeLayout("2006-01-02 15:04:05.000", "2021-10-20 13:22:25.910"),
				},
			},
			wantErr: false,
		},
		{
			name: "no rows in result set",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDbEmpty,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "error in result set",
			fields: fields{
				nucleusDb:         nil,
				machineLearningDb: machineLearningDbError,
				logger:            serverLogger,
			},
			args: args{
				ctx: context.TODO(),
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetLarBaselist(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetLarBaselist() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetLarBaselist() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPowerDealByKeys(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	lstPowerkeys := []float64{3996506, 3996507}

	dealKeysQuery, paramsDeal, err := oracle.CreateInQueryFloat64(lstPowerkeys, []interface{}{}, "pd.power_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerDealByKeysQuery := getNucPowerDealByKeysQuery(dealKeysQuery)

	var namedParamsQuery []driver.Value
	for _, value := range paramsDeal {
		valueNamed, _ := value.(sql.NamedArg)
		namedParamsQuery = append(namedParamsQuery, valueNamed)
	}

	columns := []string{"POWER_KEY", "DEAL_TYPE", "DN_DIRECTION", "TRANSACTION_DATE",
		"CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "CONTRACTNUMBER",
		"CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO", "PORTFOLIO", "UR_TRADER",
		"TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "OPTION_KEY", "CREATEDBY", "CREATE_DATE",
		"MODIFIEDBY", "MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME", "EXOTIC_FLAG"}

	mock.ExpectQuery(getNucPowerDealByKeysQuery).WithArgs(namedParamsQuery...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, "PWRNSD", "PURCHASE", parseTime("05-05-2022"),
			1601, "PJM", "PJM INTERCONNECTION LLC", "PJM",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "011-KW-BI-05701",
			"HOURLY", "EAST", "", 288, "SD - TRANS", "SROSS",
			"PPT", "NO", "NA", nil, "SROSS", parseTime("04-05-2022"),
			"SROSS", parseTime("05-05-2022"), "", "", "NA",
		).AddRow(
			3996507, "PWRNSD", "SALE", parseTime("05-05-2022"),
			10459, "MISO", "MIDWEST INDEPENDENT TRANSMISSION SYSTEM", "MIDWEST IN",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "011-KW-IS-10952",
			"HOURLY", "EAST", "", 288, "SD - TRANS", "SROSS",
			"PPT", "NO", "NA", nil, "SROSS", parseTime("04-05-2022"),
			"SROSS", parseTime("05-05-2022"), "2022-05-05", "08:18:55 AM", "NA",
		))

	dealKeysArray := []int{3996506, 3996507}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pv.pd_power_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerTradeTermModelQ := getNucPowerTradeTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"PD_POWER_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY", "PRICE_TYPE",
		"PRICE", "VOLUME", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT", "CTP_POINT_CODE", "FORMULA",
		"SCH_SCHEDULE"}

	mock.ExpectQuery(getNucPowerTradeTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, 0, parseTime("05-05-2022"), parseTime("05-05-2022"), "I",
			0, 0, "PJM", "HOURLY", "MISO", "[PJM DA LMP|40523629|HOURLY]",
			"NERC",
		).AddRow(
			3996507, 0, parseTime("05-05-2022"), parseTime("05-05-2022"), "I",
			0, 0, "MISOE", "HOURLY", "MISO/PJM", "[MISO DALMP|PJMC|HOURLY]",
			"NERC",
		))

	formulaMap := make(map[int]int)
	formulaMap[3996506] = 0
	formulaMap[3996507] = 0

	insNumQuery, _, err := oracle.CreateMapIntWhereQuery(formulaMap, []interface{}{}, "pv_pd_power_key", "pv_volume_seq")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerTradeIndexModelQ := getNucPowerTradeIndexModelQuery(insNumQuery)

	columns = []string{"PV_PD_POWER_KEY", "PV_VOLUME_SEQ", "PUBLICATION", "PUB_INDEX", "FREQUENCY"}

	// this is a small hack to allow any values to be in any order for the mock,
	// the issue is that the parameters for the query never will be in the same
	// order because it comes from iterating over the keys of a map and Go always
	// randmize them
	mock.ExpectQuery(getNucPowerTradeIndexModelQ).WithArgs(AnyValueMatch{}, AnyValueMatch{},
		AnyValueMatch{}, AnyValueMatch{}).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			3996506, 0, "PJM DA LMP", "40523629", "HOURLY",
		).AddRow(
			3996507, 0, "MISO DALMP", "PJMC", "HOURLY",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx          context.Context
		lstPowerkeys []float64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:          context.TODO(),
				lstPowerkeys: lstPowerkeys,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             3996506,
					DealType:            "PWRNSD",
					TotalQuantity:       0,
					TransactionDate:     parseTime("05-05-2022"),
					DnDirection:         "PURCHASE",
					CyCompanyKey:        1601,
					CompanyCode:         "PJM",
					Company:             "PJM",
					CompanyLongName:     "PJM INTERCONNECTION LLC",
					HsHedgeKey:          "",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "SROSS",
					CyBrokerKey:         0,
					Broker:              "NA",
					DaRtIndicator:       "",
					TzTimeZone:          "PPT",
					TzExerciseZone:      "",
					HasBroker:           "NO",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					ExercisedOptionKey:  0,
					Contract:            "011-KW-BI-05701",
					ConfirmFormat:       "HOURLY",
					CyLegalEntityKey:    10430,
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					Region:              "EAST",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
							Pool1:           "PJM",
							Product1:        "HOURLY",
							PointCode1:      "MISO",
							Pool2:           "",
							Product2:        "",
							HolidaySchedule: "NERC",
							PointCode2:      "",
							Formula1:        "[PJM DA LMP|40523629|HOURLY]",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									VolSeq:      0,
									Publication: "PJM DA LMP",
									PubIndex:    "40523629",
									Frequency:   "HOURLY",
								},
							},
							Formula2:   "",
							Indexes2:   nil,
							PriceType:  "I",
							FixedPrice: 0,
							Volume:     0,
						},
					},
					AnomalyTestResult:  "",
					CreatedAt:          parseTime("04-05-2022"),
					ModifiedAt:         parseTime("05-05-2022"),
					CreatedBy:          "SROSS",
					ModifiedBy:         "SROSS",
					OptionType:         "",
					ExecutionTime:      parseTime("01-01-0001"),
					ExoticFlag:         "NA",
					InteraffiliateFlag: "N",
					StartDate:          parseTime("01-01-0001"),
					EndDate:            parseTime("01-01-0001"),
				},
				{
					DealKey:             3996507,
					DealType:            "PWRNSD",
					TotalQuantity:       0,
					TransactionDate:     parseTime("05-05-2022"),
					DnDirection:         "SALE",
					CyCompanyKey:        10459,
					CompanyCode:         "MIDWEST IN",
					Company:             "MISO",
					CompanyLongName:     "MIDWEST INDEPENDENT TRANSMISSION SYSTEM",
					HsHedgeKey:          "",
					PrtPortfolio:        288,
					Portfolio:           "SD - TRANS",
					UrTrader:            "SROSS",
					CyBrokerKey:         0,
					Broker:              "NA",
					DaRtIndicator:       "",
					TzTimeZone:          "PPT",
					TzExerciseZone:      "",
					HasBroker:           "NO",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					ExercisedOptionKey:  0,
					Contract:            "011-KW-IS-10952",
					ConfirmFormat:       "HOURLY",
					CyLegalEntityKey:    10430,
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					Region:              "EAST",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:          0,
							BegDate:         parseTime("05-05-2022"),
							EndDate:         parseTime("05-05-2022"),
							Pool1:           "MISOE",
							Product1:        "HOURLY",
							PointCode1:      "MISO/PJM",
							Pool2:           "",
							Product2:        "",
							HolidaySchedule: "NERC",
							PointCode2:      "",
							Formula1:        "[MISO DALMP|PJMC|HOURLY]",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									VolSeq:      0,
									Publication: "MISO DALMP",
									PubIndex:    "PJMC",
									Frequency:   "HOURLY",
								},
							},
							Formula2:   "",
							Indexes2:   nil,
							PriceType:  "I",
							FixedPrice: 0,
							Volume:     0,
						},
					},
					AnomalyTestResult:  "",
					CreatedAt:          parseTime("04-05-2022"),
					ModifiedAt:         parseTime("05-05-2022"),
					CreatedBy:          "SROSS",
					ModifiedBy:         "SROSS",
					OptionType:         "",
					ExecutionTime:      parseDateTime("2022-05-05", "08:18:55 AM"),
					ExoticFlag:         "NA",
					InteraffiliateFlag: "N",
					StartDate:          parseTime("01-01-0001"),
					EndDate:            parseTime("01-01-0001"),
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerDealByKeys(tt.args.ctx, tt.args.lstPowerkeys)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerDealByKeys() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPowerSwapDealByKeys(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	lstPowerSwapkeys := []float64{1388598, 1388692}

	dealKeysQuery, paramsDeal, err := oracle.CreateInQueryFloat64(lstPowerSwapkeys, []interface{}{}, "pd.pswap_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerSwapDealByKeysQuery := getNucPowerSwapDealByKeysQuery(dealKeysQuery)

	var namedParamsQuery []driver.Value
	for _, value := range paramsDeal {
		valueNamed, _ := value.(sql.NamedArg)
		namedParamsQuery = append(namedParamsQuery, valueNamed)
	}

	columns := []string{"PSWAP_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "IB_PRT_PORTFOLIO", "IB_PORTFOLIO", "IB_UR_TRADER",
		"TZ_TIME_ZONE", "HAS_BROKER", "BROKER", "OPTION_KEY", "PPEP_PP_POOL", "PPEP_PEP_PRODUCT",
		"VOLUME", "FIXED_PRICE", "NONSTD_FLAG", "PI_PB_PUBLICATION", "PI_PUB_INDEX",
		"FRQ_FREQUENCY", "FIX_PI_PB_PUBLICATION", "FIX_PI_PUB_INDEX", "FIX_FRQ_FREQUENCY",
		"DY_BEG_DAY", "DY_END_DAY", "SCH_SCHEDULE", "CREATEDBY", "CREATE_DATE", "MODIFIEDBY",
		"MODIFY_DATE", "EXECUTION_DATE", "EXECUTION_TIME", "EXOTIC_FLAG"}

	mock.ExpectQuery(getNucPowerSwapDealByKeysQuery).WithArgs(namedParamsQuery...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1388598, "PSWPS", 800, "PURCHASE",
			parseTime("05-05-2022"), 18098, "FIMAT USA", "NEWEDGE USA LLC", "FIMAT USA",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER SWAP", "SOUTH", "", 235,
			"ERCOT B E FINANCIAL", "CWATSON", nil, "", "",
			"CPT", "YES", "INTERXCHG", nil, "ERFNH", "STD ON",
			50, 111, "Y", "ER RT LMP", "NORTH_HUB_AVG",
			"HOURLY", nil, nil, nil,
			parseTime("05-05-2022"), parseTime("05-05-2022"), "NERC", "LOB2", parseTime("05-05-2022"), "LOB2",
			parseTime("05-05-2022"), "2022-05-05", "09:08:26 AM", "No",
		).AddRow(
			1388692, "PSWPS", 21600, "PURCHASE",
			parseTime("05-05-2022"), 18098, "FIMAT USA", "NEWEDGE USA LLC", "FIMAT USA",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER SWAP", "SOUTH", "", 235,
			"ERCOT B E FINANCIAL", "CWATSON", nil, "", "",
			"CPT", "YES", "INTERXCHG", nil, "ERFNH", "STD 2x16",
			135, 30, "N", "ER RT LMP", "NORTH_HUB_AVG",
			"HOURLY", nil, nil, nil,
			parseTime("01-05-2025"), parseTime("31-05-2025"), "NERC", "LOB2", parseTime("05-05-2022"), "JKING",
			parseTime("05-05-2022"), "2022-05-05", "11:05:35 AM", "No",
		))

	dealKeysArray := []int{1388598}

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeysArray, []interface{}{}, "pswp_pswap_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerSwapDealTermModelQ := getNucPowerSwapDealTermModelQuery(dealKeysQuery)

	var namedParams []driver.Value
	for _, value := range params {
		valueNamed, _ := value.(sql.NamedArg)
		namedParams = append(namedParams, valueNamed)
	}

	columns = []string{"PSWP_PSWAP_KEY", "VOLUME_SEQ", "DY_BEG_DAY", "DY_END_DAY"}
	mock.ExpectQuery(getNucPowerSwapDealTermModelQ).WithArgs(namedParams...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			1388598, 613, parseTime("05-03-2024"), parseTime("05-03-2024"),
		).AddRow(
			1388598, 614, parseTime("06-03-2024"), parseTime("06-03-2024"),
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx              context.Context
		lstPowerSwapkeys []float64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:              context.TODO(),
				lstPowerSwapkeys: lstPowerSwapkeys,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             1388598,
					DealType:            "PSWPS",
					TotalQuantity:       800,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        18098,
					Company:             "FIMAT USA",
					CompanyLongName:     "NEWEDGE USA LLC",
					CompanyCode:         "FIMAT USA",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "POWER SWAP",
					Region:              "SOUTH",
					PrtPortfolio:        235,
					Portfolio:           "ERCOT B E FINANCIAL",
					UrTrader:            "CWATSON",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					HasBroker:           "YES",
					Broker:              "INTERXCHG",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("05-05-2022"),
					EndDate:             parseTime("05-05-2022"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     613,
							Pool1:      "ERFNH",
							Product1:   "STD ON",
							Volume:     0,
							FixedPrice: 0,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("05-03-2024"),
							EndDate:         parseTime("05-03-2024"),
							HolidaySchedule: "NERC",
						},
						{
							VolSeq:     614,
							Pool1:      "ERFNH",
							Product1:   "STD ON",
							Volume:     0,
							FixedPrice: 0,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("06-03-2024"),
							EndDate:         parseTime("06-03-2024"),
							HolidaySchedule: "NERC",
						},
					},
					CreatedBy:          "LOB2",
					CreatedAt:          parseTime("05-05-2022"),
					ModifiedBy:         "LOB2",
					ModifiedAt:         parseTime("05-05-2022"),
					ExecutionTime:      parseDateTime("2022-05-05", "09:08:26 AM"),
					ExoticFlag:         "No",
					InteraffiliateFlag: "N",
				},
				{
					DealKey:             1388692,
					DealType:            "PSWPS",
					TotalQuantity:       21600,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("05-05-2022"),
					CyCompanyKey:        18098,
					Company:             "FIMAT USA",
					CompanyLongName:     "NEWEDGE USA LLC",
					CompanyCode:         "FIMAT USA",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					Contract:            "",
					ConfirmFormat:       "POWER SWAP",
					Region:              "SOUTH",
					PrtPortfolio:        235,
					Portfolio:           "ERCOT B E FINANCIAL",
					UrTrader:            "CWATSON",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					HasBroker:           "YES",
					Broker:              "INTERXCHG",
					ExercisedOptionKey:  0,
					StartDate:           parseTime("01-05-2025"),
					EndDate:             parseTime("31-05-2025"),
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							VolSeq:     0,
							Pool1:      "ERFNH",
							Product1:   "STD 2x16",
							Volume:     135,
							FixedPrice: 30,
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "ER RT LMP",
									PubIndex:    "NORTH_HUB_AVG",
									Frequency:   "HOURLY",
								},
							},
							BegDate:         parseTime("01-05-2025"),
							EndDate:         parseTime("31-05-2025"),
							HolidaySchedule: "NERC",
						},
					},
					CreatedBy:          "LOB2",
					CreatedAt:          parseTime("05-05-2022"),
					ModifiedBy:         "JKING",
					ModifiedAt:         parseTime("05-05-2022"),
					ExecutionTime:      parseDateTime("2022-05-05", "11:05:35 AM"),
					ExoticFlag:         "No",
					InteraffiliateFlag: "N",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerSwapDealByKeys(tt.args.ctx, tt.args.lstPowerSwapkeys)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerSwapDealByKeys() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}

func TestNucleusTradeRepository_GetNucPowerOptionsDealByKeys(t *testing.T) {
	serverLogger := logger.GetServerLogger()
	serverLogger.Enable(false)

	nucleusDb, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer nucleusDb.Close()

	lstPowerOptionkeys := []float64{43435, 43441}

	dealKeysQuery, paramsDeal, err := oracle.CreateInQueryFloat64(lstPowerOptionkeys, []interface{}{}, "pd.poption_key")
	if err != nil {
		t.Fatalf("an error '%s' was not expected when creating the query", err)
	}

	getNucPowerOptionsDealByKeysQ := getNucPowerOptionsDealByKeysQuery(dealKeysQuery)

	var namedParamsQuery []driver.Value
	for _, value := range paramsDeal {
		valueNamed, _ := value.(sql.NamedArg)
		namedParamsQuery = append(namedParamsQuery, valueNamed)
	}

	columns := []string{"POPTION_KEY", "DEAL_TYPE", "TOTAL_QUANTITY", "DN_DIRECTION",
		"TRANSACTION_DATE", "CY_COMPANY_KEY", "COMPANY", "COMPANYLONGNAME", "COMPANYCODE",
		"LEGALENTITY", "LEGALENTITYLONGNAME", "CYLEGALENTITYKEY", "INTERAFFILIATE_FLAG",
		"CONTRACTNUMBER", "CONFIRMFORMAT", "REGION", "HS_HEDGE_KEY", "PRTPORTFOLIO",
		"PORTFOLIO", "UR_TRADER", "IB_PRT_PORTFOLIO", "IB_PORTFOLIO", "IB_UR_TRADER",
		"TZ_TIME_ZONE", "TZ_EXERCISE_ZONE", "HAS_BROKER", "BROKER", "PPEP_PP_POOL",
		"PPEP_PEP_PRODUCT", "CTP_POINT_CODE", "SETTLE_FORMULA", "DY_BEG_DAY", "DY_END_DAY",
		"SCH_SCHEDULE", "VOLUME", "STRIKE_PRICE", "STRIKE_PRICE_TYPE", "STRIKE_FORMULA",
		"CREATEDBY", "MODIFIEDBY", "CREATE_DATE", "MODIFY_DATE", "EXECUTION_DATE",
		"EXECUTION_TIME", "EXOTIC_FLAG",
	}
	mock.ExpectQuery(getNucPowerOptionsDealByKeysQ).WithArgs(namedParamsQuery...).
		WillReturnRows(sqlmock.NewRows(columns).AddRow(
			43435, "POPTS", 17600, "SALE",
			parseTime("23-05-2022"), 10322, "IBT", "INTERBOOK TRANSFER", "IBT",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER OPT", "EAST", "", 99478,
			"ERCOT_RETAIL", "GABDULLA", nil, "", "",
			"CPT", "CPT", "NO", "NA", "ERCNZ",
			"STD ON", "HB_NORTH", nil, parseTime("01-06-2022"), parseTime("30-06-2022"),
			"NERC", -50, 100, "F", "",
			"GABDULLA", "GABDULLA", parseTime("23-05-2022"), parseTime("23-05-2022"), "05/23/2022",
			"12:00:00 AM", "No",
		).AddRow(
			43441, "POPTS", 35200, "PURCHASE",
			parseTime("23-05-2022"), 10322, "IBT", "INTERBOOK TRANSFER", "IBT",
			"SENA", "Shell Energy North America (US), L.P.", 10430, "N",
			"", "POWER OPT", "EAST", "", 220,
			"SOUTH OTHER HR", "GABDULLA", nil, "", "",
			"CPT", "CPT", "NO", "NA", "ERCNZ",
			"STD ON", "HB_NORTH", "[MISO RTLMP|CINERGY.HUB|HOURLY]", parseTime("01-06-2022"), parseTime("30-06-2022"),
			"NERC", 100, 100, "F", "(10.5*[GD|HOU SHP CHNL|DAILY]) + 3.55",
			"GABDULLA", "GABDULLA", parseTime("23-05-2022"), parseTime("23-05-2022"), "05/23/2022",
			"12:00:00 AM", "No",
		))

	type fields struct {
		nucleusDb         *sql.DB
		machineLearningDb *sql.DB
		logger            logger.Logger
	}
	type args struct {
		ctx                context.Context
		lstPowerOptionkeys []float64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*nucleus.NucleusTradeHeaderModel
		wantErr bool
	}{
		{
			name: "regular run",
			fields: fields{
				nucleusDb:         nucleusDb,
				machineLearningDb: nil,
				logger:            serverLogger,
			},
			args: args{
				ctx:                context.TODO(),
				lstPowerOptionkeys: lstPowerOptionkeys,
			},
			want: []*nucleus.NucleusTradeHeaderModel{
				{
					DealKey:             43435,
					DealType:            "POPTS",
					TotalQuantity:       17600,
					DnDirection:         "SALE",
					TransactionDate:     parseTime("23-05-2022"),
					CyCompanyKey:        10322,
					Company:             "IBT",
					CompanyLongName:     "INTERBOOK TRANSFER",
					CompanyCode:         "IBT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "",
					ConfirmFormat:       "POWER OPT",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        99478,
					Portfolio:           "ERCOT_RETAIL",
					UrTrader:            "GABDULLA",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					TzExerciseZone:      "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-06-2022"),
					EndDate:             parseTime("30-06-2022"),
					CreatedBy:           "GABDULLA",
					ModifiedBy:          "GABDULLA",
					CreatedAt:           parseTime("23-05-2022"),
					ModifiedAt:          parseTime("23-05-2022"),
					ExecutionTime:       parseDateTime("05/23/2022", "12:00:00 AM"),
					ExoticFlag:          "No",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							Pool1:           "ERCNZ",
							Product1:        "STD ON",
							PointCode1:      "HB_NORTH",
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("30-06-2022"),
							HolidaySchedule: "NERC",
							Volume:          -50,
							FixedPrice:      100,
							PriceType:       "F",
						},
					},
				},
				{
					DealKey:             43441,
					DealType:            "POPTS",
					TotalQuantity:       35200,
					DnDirection:         "PURCHASE",
					TransactionDate:     parseTime("23-05-2022"),
					CyCompanyKey:        10322,
					Company:             "IBT",
					CompanyLongName:     "INTERBOOK TRANSFER",
					CompanyCode:         "IBT",
					LegalEntity:         "SENA",
					LegalEntityLongName: "Shell Energy North America (US), L.P.",
					CyLegalEntityKey:    10430,
					InteraffiliateFlag:  "N",
					Contract:            "",
					ConfirmFormat:       "POWER OPT",
					Region:              "EAST",
					HsHedgeKey:          "",
					PrtPortfolio:        220,
					Portfolio:           "SOUTH OTHER HR",
					UrTrader:            "GABDULLA",
					IbPrtPortfolio:      0,
					IbPortfolio:         "",
					IbUrTrader:          "",
					TzTimeZone:          "CPT",
					TzExerciseZone:      "CPT",
					HasBroker:           "NO",
					Broker:              "NA",
					StartDate:           parseTime("01-06-2022"),
					EndDate:             parseTime("30-06-2022"),
					CreatedBy:           "GABDULLA",
					ModifiedBy:          "GABDULLA",
					CreatedAt:           parseTime("23-05-2022"),
					ModifiedAt:          parseTime("23-05-2022"),
					ExecutionTime:       parseDateTime("05/23/2022", "12:00:00 AM"),
					ExoticFlag:          "No",
					Terms: []*nucleus.NucleusTradeTermModel{
						{
							Pool1:           "ERCNZ",
							Product1:        "STD ON",
							PointCode1:      "HB_NORTH",
							BegDate:         parseTime("01-06-2022"),
							EndDate:         parseTime("30-06-2022"),
							HolidaySchedule: "NERC",
							Volume:          100,
							FixedPrice:      100,
							PriceType:       "F",
							Indexes1: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "MISO RTLMP",
									PubIndex:    "CINERGY.HUB",
									Frequency:   "HOURLY",
								},
							},
							Indexes2: []*nucleus.NucleusTradeIndexModel{
								{
									Publication: "GD",
									PubIndex:    "HOU SHP CHNL",
									Frequency:   "DAILY",
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &NucleusTradeRepository{
				nucleusDb:         tt.fields.nucleusDb,
				machineLearningDb: tt.fields.machineLearningDb,
				logger:            tt.fields.logger,
			}
			got, err := repo.GetNucPowerOptionsDealByKeys(tt.args.ctx, tt.args.lstPowerOptionkeys)
			if (err != nil) != tt.wantErr {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != len(tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() len(got) %d != len(tt.want) %d", len(got), len(tt.want))
				return
			}

			for _, wantedValue := range tt.want {
				exists := false
				for _, value := range got {
					if wantedValue.DealKey == value.DealKey {
						exists = true
						if len(value.Terms) != len(wantedValue.Terms) {
							t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() len(value.Terms) %d != len(wantedValue.Terms) %d", len(value.Terms), len(wantedValue.Terms))
							return
						}

						for _, wantedTerm := range wantedValue.Terms {
							termExists := false
							for _, valueTerm := range value.Terms {
								if wantedTerm.VolSeq == valueTerm.VolSeq {
									termExists = true

									if len(wantedTerm.Indexes1) != len(valueTerm.Indexes1) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() len(wantedTerm.Indexes1) %d != len(valueTerm.Indexes1) %d", len(wantedTerm.Indexes1), len(valueTerm.Indexes1))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes1 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes1 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if len(wantedTerm.Indexes2) != len(valueTerm.Indexes2) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() len(wantedTerm.Indexes2) %d != len(valueTerm.Indexes2) %d", len(wantedTerm.Indexes2), len(valueTerm.Indexes2))
										return
									}

									for _, wantedIndex := range wantedTerm.Indexes2 {
										indexExists := false
										for _, valueIndex := range valueTerm.Indexes2 {
											if reflect.DeepEqual(wantedIndex, valueIndex) {
												indexExists = true
											}
										}
										if !indexExists {
											t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wanted index in term does not exist %s in wantedValue.DealKey %d, wantedTerm.VolSeq %d", wantedIndex.PubIndex, wantedValue.DealKey, wantedTerm.VolSeq)
											return
										}
									}

									if !reflect.DeepEqual(wantedTerm, valueTerm) {
										t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wantedTerm = %+v, valueTerm = %+v", wantedTerm, valueTerm)
										return
									}
								}
							}
							if !termExists {
								t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wanted term with VolSeq %d value does not exist in wantedValue.DealKey %d", wantedTerm.VolSeq, wantedValue.DealKey)
								return
							}
						}

						if !reflect.DeepEqual(wantedValue, value) {
							t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wantedValue = %+v, value = %+v", wantedValue, value)
							return
						}
					}
				}
				if !exists {
					t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() wanted value does not exist %d", wantedValue.DealKey)
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NucleusTradeRepository.GetNucPowerOptionsDealByKeys() got = %+v, want = %+v", got, tt.want)
				return
			}
		})
	}
}
