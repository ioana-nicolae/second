package power

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"

	"github.com/sede-x/RogerRogerAnomalyDetector/db/oracle"
	"github.com/sede-x/RogerRogerAnomalyDetector/logger"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/common"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/nucleus"
)

const (
	formulaPattern = ("\\[.+\\|.+\\|.+\\]")
)

func parseExecutionDateTime(executionDate string, executionTime string) (time.Time, error) {
	executionDateString := strings.Split(executionDate, " ")[0]
	executionTimeString := executionDateString + " " + executionTime
	execTime, err := time.Parse("2006-01-02 03:04:05 PM", executionTimeString)
	if err != nil {
		execTime, err = time.Parse("01/02/2006 03:04:05 PM", executionTimeString)
		if err != nil {
			return time.Time{}, err
		}
	}
	return execTime, err
}

func parseExecutionDate(executionDate string) (time.Time, error) {
	execTime, err := time.Parse("2006-01-02", executionDate)
	if err != nil {
		execTime, err = time.Parse("01/02/2006", executionDate)
		if err != nil {
			return time.Time{}, err
		}
	}
	return execTime, err
}

type NucleusTradeRepository struct {
	nucleusDb         *sql.DB
	machineLearningDb *sql.DB
	logger            logger.Logger
}

func NewNucleusTradeRepository(nucleusDb *sql.DB, machineLearningDb *sql.DB, logger logger.Logger) *NucleusTradeRepository {
	return &NucleusTradeRepository{
		nucleusDb,
		machineLearningDb,
		logger,
	}
}

func (repo *NucleusTradeRepository) GetNucPowerDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucPowerDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucPowerDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)
	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker, executionDate, executionTime, exoticFlag sql.NullString
		var optionKey sql.NullInt32

		if err := rows.Scan(&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company,
			&headerModel.CompanyLongName, &headerModel.CompanyCode, &headerModel.LegalEntity,
			&headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone,
			&hasBroker, &broker, &optionKey, &headerModel.CreatedBy,
			&headerModel.CreatedAt, &headerModel.ModifiedBy, &headerModel.ModifiedAt,
			&executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if optionKey.Valid {
			headerModel.ExercisedOptionKey = int(optionKey.Int32)
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		headerModel.InteraffiliateFlag = "N"

		dealKeys = append(dealKeys, headerModel.DealKey)
		headerModelsMap[headerModel.DealKey] = &headerModel
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucPowerDealTradeTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucPowerDealTradeTermModel: ", err)
			return nil, err
		}

		for index := range headerModelsMap {
			headerModelsMap[index].Terms = termModelsMap[index]
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucPowerDealTradeTermModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucPowerDealTradeTermModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModels := make(map[int][]*nucleus.NucleusTradeTermModel)

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "pv.pd_power_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucPowerTradeTermModelQuery(dealKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerTradeTermModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	formulaMap := make(map[int]int)
	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel

		var powerKey int
		var formula sql.NullString

		if err := rows.Scan(
			&powerKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate, &termModel.PriceType,
			&termModel.FixedPrice, &termModel.Volume, &termModel.Pool1, &termModel.Product1,
			&termModel.PointCode1, &formula, &termModel.HolidaySchedule,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		if formula.Valid && formula.String != "" {
			termModel.Formula1 = formula.String
			formulaMap[powerKey] = termModel.VolSeq
		}

		termModels[powerKey] = append(termModels[powerKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	indexModelsMap, err := repo.getNucPowerTradeIndexModel(ctx, formulaMap)
	if err != nil {
		logger.Debugln("error getting getNucPowerTradeIndexModel: ", err)
		return nil, err
	}

	for dealKey, indexModels := range indexModelsMap {
		for _, indexModel := range indexModels {
			if termModels, ok := termModels[dealKey]; ok {
				for _, termModel := range termModels {
					if termModel.VolSeq == indexModel.VolSeq {
						termModel.Indexes1 = append(termModel.Indexes1, indexModel)
					}
				}
			}
		}
	}

	return termModels, nil
}

func (repo *NucleusTradeRepository) getNucPowerTradeIndexModel(ctx context.Context, formulaMap map[int]int) (map[int][]*nucleus.NucleusTradeIndexModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucPowerTradeIndexModel")

	if len(formulaMap) == 0 {
		return nil, nil
	}

	insNumResult := make(map[int][]*nucleus.NucleusTradeIndexModel)

	insNumQuery, params, err := oracle.CreateMapIntWhereQuery(formulaMap, []interface{}{}, "pv_pd_power_key", "pv_volume_seq")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucPowerTradeIndexModelQuery(insNumQuery)

	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerTradeIndexModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termIndex nucleus.NucleusTradeIndexModel
		var dealKey int
		if err := rows.Scan(
			&dealKey, &termIndex.VolSeq, &termIndex.Publication,
			&termIndex.PubIndex, &termIndex.Frequency,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		insNumResult[dealKey] = append(insNumResult[dealKey], &termIndex)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return insNumResult, nil
}

func (repo *NucleusTradeRepository) GetNucPowerSwapDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerSwapDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucPowerSwapDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucPowerSwapDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)
	initialTermModelsMap := make(map[int]*nucleus.NucleusTradeTermModel)

	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel
		var idxModel, idxModel2 nucleus.NucleusTradeIndexModel

		var interaffiliateFlag, contract, confirmFormat,
			hsHedgeKey, ibPortfolio, ibUrTrader, hasBroker,
			broker, executionDate, executionTime, exoticFlag sql.NullString
		var totalQuantity, fixedPrice sql.NullFloat64
		var ibPrtPortfolio, optionKey sql.NullInt32

		var nonstdFlag string

		var fixPiPbPublication, fixPiPubIndex, fixFrqFrequency sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName,
			&headerModel.CompanyCode, &headerModel.LegalEntity, &headerModel.LegalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &ibPrtPortfolio,
			&ibPortfolio, &ibUrTrader, &headerModel.TzTimeZone, &hasBroker,
			&broker, &optionKey, &pvModel.Pool1, &pvModel.Product1,
			&pvModel.Volume, &fixedPrice, &nonstdFlag, &idxModel.Publication,
			&idxModel.PubIndex, &idxModel.Frequency, &fixPiPbPublication,
			&fixPiPubIndex, &fixFrqFrequency, &headerModel.StartDate, &headerModel.EndDate,
			&pvModel.HolidaySchedule, &headerModel.CreatedBy, &headerModel.CreatedAt, &headerModel.ModifiedBy,
			&headerModel.ModifiedAt, &executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if ibPrtPortfolio.Valid {
			headerModel.IbPrtPortfolio = int(ibPrtPortfolio.Int32)
		}

		if ibPortfolio.Valid {
			headerModel.IbPortfolio = ibPortfolio.String
		}

		if ibUrTrader.Valid {
			headerModel.IbUrTrader = ibUrTrader.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if optionKey.Valid {
			headerModel.ExercisedOptionKey = int(optionKey.Int32)
		}

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		if fixPiPbPublication.Valid && fixPiPubIndex.Valid && fixFrqFrequency.Valid {
			idxModel2.Publication = fixPiPbPublication.String
			idxModel2.PubIndex = fixPiPubIndex.String
			idxModel2.Frequency = fixFrqFrequency.String
			pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)
		}

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate
		pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)

		if nonstdFlag == "N" {
			pvModel.VolSeq = 0
			headerModel.Terms = append(headerModel.Terms, &pvModel)
		} else {
			initialTermModelsMap[headerModel.DealKey] = &pvModel
			headerModelsMap[headerModel.DealKey] = &headerModel
			dealKeys = append(dealKeys, headerModel.DealKey)
		}

		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucPowerSwapDealTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucPowerSwapDealTermModel: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			for _, termModel := range termModels {
				termModel.Pool1 = initialTermModelsMap[dealKey].Pool1
				termModel.Product1 = initialTermModelsMap[dealKey].Product1
				termModel.Indexes1 = initialTermModelsMap[dealKey].Indexes1
				termModel.Indexes2 = initialTermModelsMap[dealKey].Indexes2
				termModel.HolidaySchedule = initialTermModelsMap[dealKey].HolidaySchedule

				headerModelsMap[dealKey].Terms = append(headerModelsMap[dealKey].Terms, termModel)
			}
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucPowerSwapDealTermModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucPowerSwapDealTermModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModelMap := make(map[int][]*nucleus.NucleusTradeTermModel)

	insNumQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "pswp_pswap_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucPowerSwapDealTermModelQuery(insNumQuery)

	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerSwapDealTermModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel

		var dealKey int

		if err := rows.Scan(
			&dealKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		termModelMap[dealKey] = append(termModelMap[dealKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModelMap, nil
}

func (repo *NucleusTradeRepository) GetNucPowerOptionsDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerOptionsDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucPowerOptionsDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucPowerOptionsDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	formulaRegex, err := regexp.Compile(formulaPattern)
	if err != nil {
		logger.Debugln("error when compiling regex: ", err)
		return nil, err
	}

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker, executionDate, executionTime, exoticFlag,
			interaffiliateFlag, ibPortfolio, ibUrTrader,
			settleFormula, strikeFormula sql.NullString
		var ibPrtPortfolio sql.NullInt32
		var totalQuantity, fixedPrice sql.NullFloat64

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName,
			&headerModel.CompanyCode, &headerModel.LegalEntity, &headerModel.LegalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &ibPrtPortfolio, &ibPortfolio,
			&ibUrTrader, &headerModel.TzTimeZone, &headerModel.TzExerciseZone, &hasBroker,
			&broker, &pvModel.Pool1, &pvModel.Product1, &pvModel.PointCode1,
			&settleFormula, &headerModel.StartDate, &headerModel.EndDate, &pvModel.HolidaySchedule,
			&pvModel.Volume, &fixedPrice, &pvModel.PriceType, &strikeFormula,
			&headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
			&executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if ibPrtPortfolio.Valid {
			headerModel.IbPrtPortfolio = int(ibPrtPortfolio.Int32)
		}

		if ibPortfolio.Valid {
			headerModel.IbPortfolio = ibPortfolio.String
		}

		if ibUrTrader.Valid {
			headerModel.IbUrTrader = ibUrTrader.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate
		pvModel.VolSeq = 0

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if settleFormula.Valid && settleFormula.String != "" {
			formula := formulaRegex.FindString(settleFormula.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
			}
		}

		if strikeFormula.Valid && strikeFormula.String != "" {
			formula := formulaRegex.FindString(strikeFormula.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel)
			}
		}

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucCapacityDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucCapacityDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucCapacityDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucCapacityDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)
	initialTermModelsMap := make(map[int]*nucleus.NucleusTradeTermModel)

	var dealKeys []int
	var formulaDealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel

		var interaffiliateFlag, contract, confirmFormat,
			hsHedgeKey, hasBroker, broker, executionDate,
			executionTime, energyFormula sql.NullString
		var totalQuantity, charge sql.NullFloat64

		var nonstdFlag string

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName,
			&headerModel.CompanyCode, &headerModel.LegalEntity, &headerModel.LegalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone,
			&hasBroker, &broker, &nonstdFlag, &pvModel.PriceType,
			&charge, &pvModel.Volume, &energyFormula, &pvModel.Pool1,
			&pvModel.Product1, &pvModel.PointCode1, &headerModel.StartDate, &headerModel.EndDate, &pvModel.HolidaySchedule,
			&headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
			&executionDate, &executionTime,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if charge.Valid {
			pvModel.FixedPrice = charge.Float64
		}

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate

		headerModel.ExoticFlag = "NA"

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if energyFormula.Valid {
			pvModel.Formula1 = energyFormula.String
			formulaDealKeys = append(formulaDealKeys, headerModel.DealKey)
		}

		if nonstdFlag == "N" {
			pvModel.VolSeq = 0
			headerModel.Terms = append(headerModel.Terms, &pvModel)
		} else {
			initialTermModelsMap[headerModel.DealKey] = &pvModel
			headerModelsMap[headerModel.DealKey] = &headerModel
			dealKeys = append(dealKeys, headerModel.DealKey)
		}

		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucCapacityDealTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucCapacityDealTermModel: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			for _, termModel := range termModels {
				termModel.PriceType = initialTermModelsMap[dealKey].PriceType
				termModel.Pool1 = initialTermModelsMap[dealKey].Pool1
				termModel.Product1 = initialTermModelsMap[dealKey].Product1
				termModel.PointCode1 = initialTermModelsMap[dealKey].PointCode1
				termModel.HolidaySchedule = initialTermModelsMap[dealKey].HolidaySchedule

				headerModelsMap[dealKey].Terms = append(headerModelsMap[dealKey].Terms, termModel)
			}
		}
	}

	if len(formulaDealKeys) > 0 {
		indexModelsMap, err := repo.getNucCapacityDealIndexModel(ctx, formulaDealKeys)
		if err != nil {
			logger.Debugln("error getting getNucCapacityDealIndexModel: ", err)
			return nil, err
		}

		for dealKey, indexModels := range indexModelsMap {
			headerModelsMap[dealKey].Terms[0].Indexes1 = indexModels
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucCapacityDealTermModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucCapacityDealTermModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModelMap := make(map[int][]*nucleus.NucleusTradeTermModel)

	insNumQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "cpd_capacity_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucCapacityDealTermModelQuery(insNumQuery)

	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucCapacityDealTermModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel

		var dealKey int

		if err := rows.Scan(
			&dealKey, &termModel.BegDate, &termModel.EndDate,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		termModel.VolSeq = len(termModelMap[dealKey])
		termModelMap[dealKey] = append(termModelMap[dealKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModelMap, nil
}

func (repo *NucleusTradeRepository) getNucCapacityDealIndexModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeIndexModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucCapacityDealIndexModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	indexModelMap := make(map[int][]*nucleus.NucleusTradeIndexModel)

	insNumQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "cpd_capacity_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucCapacityDealIndexModelQuery(insNumQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucCapacityDealIndexModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var indexModel nucleus.NucleusTradeIndexModel

		var dealKey int

		if err := rows.Scan(
			&dealKey, &indexModel.Publication, &indexModel.PubIndex, &indexModel.Frequency,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		indexModel.VolSeq = len(indexModelMap[dealKey])
		indexModelMap[dealKey] = append(indexModelMap[dealKey], &indexModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return indexModelMap, nil
}

func (repo *NucleusTradeRepository) GetNucPTPDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPTPDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucPTPDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucPTPDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel
		var idxModel, idxModel2 nucleus.NucleusTradeIndexModel

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey,
			&headerModel.PrtPortfolio, &headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone,
			&hasBroker, &broker, &pvModel.BegDate, &pvModel.Pool1, &pvModel.Product1,
			&idxModel.Publication, &idxModel.PubIndex, &idxModel2.Publication, &idxModel2.PubIndex,
			&headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		pvModel.EndDate = pvModel.BegDate

		idxModel.Frequency = "HOURLY"
		idxModel2.Frequency = "HOURLY"

		headerModel.InteraffiliateFlag = "N"

		pvModel.VolSeq = 0
		pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
		pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)

		headerModel.Terms = append(headerModel.Terms, &pvModel)

		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucEmissionDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucEmissionDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucEmissionDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucEmissionDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)

	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker, executionDate, executionTime, tzTimeZone sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey,
			&headerModel.PrtPortfolio, &headerModel.Portfolio, &headerModel.UrTrader, &tzTimeZone,
			&hasBroker, &broker, &headerModel.CreatedBy, &headerModel.ModifiedBy,
			&headerModel.CreatedAt, &headerModel.ModifiedAt, &executionDate, &executionTime,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if tzTimeZone.Valid {
			headerModel.TzTimeZone = tzTimeZone.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		headerModel.ExoticFlag = "NA"
		headerModel.InteraffiliateFlag = "N"

		dealKeys = append(dealKeys, headerModel.DealKey)
		headerModels = append(headerModels, &headerModel)
		headerModelsMap[headerModel.DealKey] = &headerModel
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucEmissionDealListTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucEmissionDealListTermModel: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			headerModelsMap[dealKey].Terms = termModels
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucEmissionDealListTermModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucEmissionDealListTermModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModelMap := make(map[int][]*nucleus.NucleusTradeTermModel)

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "pv.ed_emission_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucEmissionDealListTermModelQuery(dealKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucEmissionDealListTermModelQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	formulaRegex, err := regexp.Compile(formulaPattern)
	if err != nil {
		logger.Debugln("error when compiling regex: ", err)
		return nil, err
	}

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel
		var dealKey int

		var ctpPointCode, formula sql.NullString

		if err := rows.Scan(
			&dealKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate,
			&termModel.PriceType, &termModel.FixedPrice, &termModel.Volume, &termModel.Product1,
			&ctpPointCode, &formula,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		if ctpPointCode.Valid {
			termModel.PointCode1 = ctpPointCode.String
		}

		if formula.Valid && formula.String != "" {
			termModel.Formula1 = formula.String

			formula := formulaRegex.FindString(formula.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				termModel.Indexes1 = append(termModel.Indexes1, &idxModel)
			}
		}

		termModelMap[dealKey] = append(termModelMap[dealKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModelMap, nil
}

func (repo *NucleusTradeRepository) GetNucEmissionOptionDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucEmissionOptionDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucEmissionOptionDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucEmissionOptionDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	var emissionKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel

		var emissionKey int

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker, executionDate, executionTime, tzTimeZone sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey,
			&headerModel.PrtPortfolio, &headerModel.Portfolio, &headerModel.UrTrader, &tzTimeZone,
			&hasBroker, &broker, &emissionKey, &pvModel.FixedPrice,
			&pvModel.Volume, &headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt,
			&headerModel.ModifiedAt, &executionDate, &executionTime,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if tzTimeZone.Valid {
			headerModel.TzTimeZone = tzTimeZone.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		headerModel.ExoticFlag = "NA"
		headerModel.InteraffiliateFlag = "N"

		pvModel.VolSeq = emissionKey

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		emissionKeys = append(emissionKeys, emissionKey)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(emissionKeys) > 0 {
		termModelsMap, err := repo.getNucEmissionOptionDealTermList(ctx, emissionKeys)
		if err != nil {
			logger.Debugln("error getting getNucEmissionOptionDealTermList: ", err)
			return nil, err
		}

		for _, headerModel := range headerModels {
			emissionKey := headerModel.Terms[0].VolSeq

			var modelTerms []*nucleus.NucleusTradeTermModel
			for _, termModel := range termModelsMap[emissionKey] {
				termModel.FixedPrice = headerModel.Terms[0].FixedPrice
				termModel.Volume = headerModel.Terms[0].Volume
				modelTerms = append(modelTerms, termModel)
			}
			headerModel.Terms = modelTerms
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucEmissionOptionDealTermList(ctx context.Context, emissionKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucEmissionOptionDealTermLists")

	if len(emissionKeys) == 0 {
		return nil, nil
	}

	termModelMap := make(map[int][]*nucleus.NucleusTradeTermModel)

	emissionKeysQuery, params, err := oracle.CreateInQueryInt(emissionKeys, []interface{}{}, "pv.ed_emission_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucEmissionOptionDealTermListQuery(emissionKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucEmissionOptionDealTermListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel
		var emissionKey int

		var ctpPointCode sql.NullString

		if err := rows.Scan(
			&emissionKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate,
			&ctpPointCode, &termModel.Product1,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		if ctpPointCode.Valid {
			termModel.PointCode1 = ctpPointCode.String
		}

		termModelMap[emissionKey] = append(termModelMap[emissionKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModelMap, nil
}

func (repo *NucleusTradeRepository) GetNucSpreadOptionsDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucSpreadOptionsDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucSpreadOptionsDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucSpreadOptionsDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	formulaRegex, err := regexp.Compile(formulaPattern)
	if err != nil {
		logger.Debugln("error when compiling regex: ", err)
		return nil, err
	}

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel

		var contract, confirmFormat, hsHedgeKey, hasBroker, broker, executionDate,
			executionTime, tzTimeZone, ibPortfolio, ibUrTrader, formula1, formula2,
			pool1, pool2, product1, product2, exoticFlag, pointCode1 sql.NullString
		var ibPrtPortfolio sql.NullInt32
		var fixedPrice sql.NullFloat64

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &ibPrtPortfolio, &ibPortfolio,
			&ibUrTrader, &tzTimeZone, &hasBroker, &broker, &headerModel.StartDate, &headerModel.EndDate,
			&pvModel.HolidaySchedule, &formula1, &formula2, &pool1, &pool2, &product1, &product2,
			&pvModel.Volume, &fixedPrice, &headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt,
			&headerModel.ModifiedAt, &executionDate, &executionTime, &exoticFlag, &pointCode1,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if ibPrtPortfolio.Valid {
			headerModel.IbPrtPortfolio = int(ibPrtPortfolio.Int32)
		}

		if ibPortfolio.Valid {
			headerModel.IbPortfolio = ibPortfolio.String
		}

		if ibUrTrader.Valid {
			headerModel.IbUrTrader = ibUrTrader.String
		}

		if tzTimeZone.Valid {
			headerModel.TzTimeZone = tzTimeZone.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if pool1.Valid {
			pvModel.Pool1 = pool1.String
		}

		if pool2.Valid {
			pvModel.Pool2 = pool2.String
		}

		if product1.Valid {
			pvModel.Product1 = product1.String
		}

		if product2.Valid {
			pvModel.Product2 = product2.String
		}

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		if pointCode1.Valid {
			pvModel.PointCode1 = pointCode1.String
		}

		headerModel.InteraffiliateFlag = "N"

		pvModel.VolSeq = 0
		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate

		if formula1.Valid && formula1.String != "" {
			formula := formulaRegex.FindString(formula1.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
			}
		}

		if formula2.Valid && formula2.String != "" {
			formula := formulaRegex.FindString(formula2.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel)
			}
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucHeatRateSwapsDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucHeatRateSwapsDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucHeatRateSwapsDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucHeatRateSwapsDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel
		var idxModel, idxModel2 nucleus.NucleusTradeIndexModel

		var contract, confirmFormat, hsHedgeKey, hasBroker, broker, executionDate,
			executionTime, tzTimeZone, interaffiliateFlag, publication1, pubIndex1,
			publication2, pubIndex2, frequency2 sql.NullString
		var exercisedOptionKey sql.NullInt32
		var totalQuantity, volume sql.NullFloat64

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName,
			&headerModel.CompanyCode, &headerModel.LegalEntity, &headerModel.LegalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract, &confirmFormat,
			&headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio, &headerModel.Portfolio,
			&headerModel.UrTrader, &tzTimeZone, &hasBroker, &broker, &exercisedOptionKey,
			&pvModel.Pool1, &pvModel.Product1, &publication1, &pubIndex1, &publication2,
			&pubIndex2, &frequency2, &headerModel.StartDate, &headerModel.EndDate,
			&pvModel.HolidaySchedule, &volume, &headerModel.CreatedBy, &headerModel.ModifiedBy,
			&headerModel.CreatedAt, &headerModel.ModifiedAt, &executionDate, &executionTime,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if tzTimeZone.Valid {
			headerModel.TzTimeZone = tzTimeZone.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if exercisedOptionKey.Valid {
			headerModel.ExercisedOptionKey = int(exercisedOptionKey.Int32)
		}

		if publication1.Valid {
			idxModel.Publication = publication1.String
		}

		if pubIndex1.Valid {
			idxModel.PubIndex = pubIndex1.String
		}

		if publication2.Valid {
			idxModel2.Publication = publication2.String
		}

		if pubIndex2.Valid {
			idxModel2.PubIndex = pubIndex2.String
		}

		if frequency2.Valid {
			idxModel2.Frequency = frequency2.String
		}

		pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
		pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)
		pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)

		if volume.Valid {
			pvModel.Volume = volume.Float64
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		headerModel.ExoticFlag = "NA"
		pvModel.VolSeq = 0

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucTCCFTRSDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time, strDealType string) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucTCCFTRSDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucTCCFTRSDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("strDealType", strDealType), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucTCCFTRSDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel
		var idxModel, idxModel2 nucleus.NucleusTradeIndexModel

		var contract, confirmFormat, hsHedgeKey, hasBroker, broker sql.NullString
		var volume, fixedPrice sql.NullFloat64

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone, &hasBroker, &broker,
			&pvModel.BegDate, &pvModel.EndDate, &pvModel.HolidaySchedule, &volume,
			&fixedPrice, &pvModel.Pool1, &pvModel.Product1, &idxModel.Publication,
			&idxModel.Frequency, &idxModel.PubIndex, &idxModel2.PubIndex, &headerModel.CreatedBy,
			&headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if volume.Valid {
			pvModel.Volume = volume.Float64
		}

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		idxModel2.Publication = idxModel.Publication
		idxModel2.Frequency = idxModel.Frequency

		headerModel.ExoticFlag = "NA"
		headerModel.InteraffiliateFlag = "N"

		pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
		pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)
		pvModel.VolSeq = 0

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucTransmissionDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucTransmissionDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucTransmissionDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucTransmissionDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)

	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var contract, confirmFormat, hsHedgeKey, hasBroker, broker, executionTime sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone, &hasBroker,
			&broker, &headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt,
			&headerModel.ModifiedAt, &executionTime,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if executionTime.Valid {
			time, err := parseExecutionDate(executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}

			headerModel.ExecutionTime = time
		}

		headerModel.ExoticFlag = "NA"
		headerModel.InteraffiliateFlag = "N"

		dealKeys = append(dealKeys, headerModel.DealKey)
		headerModelsMap[headerModel.DealKey] = &headerModel
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucTransmissionDealTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucTransmissionDealTermModel: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			headerModelsMap[dealKey].Terms = termModels
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucTransmissionDealTermModel(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucTransmissionDealTermModel")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModels := make(map[int][]*nucleus.NucleusTradeTermModel)

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "pv.td_trans_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucTransmissionDealTermListQuery(dealKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucTransmissionDealTermListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel
		var dealKey int

		if err := rows.Scan(
			&dealKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate,
			&termModel.Volume, &termModel.Product1, &termModel.Pool1, &termModel.PointCode1,
			&termModel.Pool2, &termModel.PointCode2, &termModel.HolidaySchedule,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		termModel.PriceType = "F"

		termModels[dealKey] = append(termModels[dealKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModels, nil
}

func (repo *NucleusTradeRepository) GetNucMiscChargeDealList(ctx context.Context, lastRunTime time.Time, tradeDate time.Time) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucMiscChargeDealList")

	rows, err := repo.nucleusDb.QueryContext(ctx, getNucMiscChargeDealListQuery, sql.Named("tradeDate", tradeDate), sql.Named("lastRunTime", lastRunTime))
	if err != nil {
		logger.Debugln("error got when executing getNucMiscChargeDealListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)

	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var contract, confirmFormat, hsHedgeKey, hasBroker, broker, region, tzTimeZone sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection, &headerModel.TransactionDate,
			&headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName, &headerModel.CompanyCode,
			&headerModel.LegalEntity, &headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey,
			&contract, &confirmFormat, &region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &tzTimeZone, &hasBroker, &broker,
			&headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if region.Valid {
			headerModel.Region = region.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if tzTimeZone.Valid {
			headerModel.TzTimeZone = tzTimeZone.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		headerModel.InteraffiliateFlag = "N"

		dealKeys = append(dealKeys, headerModel.DealKey)
		headerModelsMap[headerModel.DealKey] = &headerModel
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucMiscChargeDealTermList(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucMiscChargeDealTermList: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			headerModelsMap[dealKey].Terms = termModels
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) getNucMiscChargeDealTermList(ctx context.Context, dealKeys []int) (map[int][]*nucleus.NucleusTradeTermModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "getNucMiscChargeDealTermList")

	if len(dealKeys) == 0 {
		return nil, nil
	}

	termModels := make(map[int][]*nucleus.NucleusTradeTermModel)

	dealKeysQuery, params, err := oracle.CreateInQueryInt(dealKeys, []interface{}{}, "pv.mc_misc_charge_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryInt: ", err)
		return nil, err
	}

	query := getNucMiscChargeDealTermListQuery(dealKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucMiscChargeDealTermListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var termModel nucleus.NucleusTradeTermModel
		var dealKey int

		if err := rows.Scan(
			&dealKey, &termModel.VolSeq, &termModel.BegDate, &termModel.EndDate,
			&termModel.Volume,
		); err != nil {
			logger.Debugln("error when scanning rows: ", err)
			return nil, err
		}

		termModels[dealKey] = append(termModels[dealKey], &termModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return termModels, nil
}

func (repo *NucleusTradeRepository) GetNucPowerDealByKeys(ctx context.Context, lstPowerkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerDealByKeys")

	dealKeysQuery, params, err := oracle.CreateInQueryFloat64(lstPowerkeys, []interface{}{}, "pd.power_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryFloat64: ", err)
		return nil, err
	}

	query := getNucPowerDealByKeysQuery(dealKeysQuery)

	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerDealByKeys: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)
	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel

		var contract, confirmFormat, hsHedgeKey, hasBroker,
			broker, executionDate, executionTime, exoticFlag sql.NullString
		var optionKey sql.NullInt32

		if err := rows.Scan(&headerModel.DealKey, &headerModel.DealType, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company,
			&headerModel.CompanyLongName, &headerModel.CompanyCode, &headerModel.LegalEntity,
			&headerModel.LegalEntityLongName, &headerModel.CyLegalEntityKey, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &headerModel.TzTimeZone,
			&hasBroker, &broker, &optionKey, &headerModel.CreatedBy,
			&headerModel.CreatedAt, &headerModel.ModifiedBy, &headerModel.ModifiedAt,
			&executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if optionKey.Valid {
			headerModel.ExercisedOptionKey = int(optionKey.Int32)
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		headerModel.InteraffiliateFlag = "N"

		dealKeys = append(dealKeys, headerModel.DealKey)
		headerModelsMap[headerModel.DealKey] = &headerModel
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucPowerDealTradeTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucPowerDealTradeTermModel: ", err)
			return nil, err
		}

		for index := range headerModelsMap {
			headerModelsMap[index].Terms = termModelsMap[index]
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucPowerSwapDealByKeys(ctx context.Context, lstPowerSwapkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerSwapDealByKeys")

	dealKeysQuery, params, err := oracle.CreateInQueryFloat64(lstPowerSwapkeys, []interface{}{}, "pd.pswap_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryFloat64: ", err)
		return nil, err
	}

	query := getNucPowerSwapDealByKeysQuery(dealKeysQuery)

	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerSwapDealByKeys: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel
	headerModelsMap := make(map[int]*nucleus.NucleusTradeHeaderModel)
	initialTermModelsMap := make(map[int]*nucleus.NucleusTradeTermModel)

	var dealKeys []int

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel
		var idxModel, idxModel2 nucleus.NucleusTradeIndexModel

		var interaffiliateFlag, contract, confirmFormat,
			hsHedgeKey, ibPortfolio, ibUrTrader, hasBroker,
			broker, executionDate, executionTime, exoticFlag sql.NullString
		var totalQuantity, fixedPrice sql.NullFloat64
		var ibPrtPortfolio, optionKey sql.NullInt32

		var nonstdFlag string

		var fixPiPbPublication, fixPiPubIndex, fixFrqFrequency sql.NullString

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &headerModel.Company, &headerModel.CompanyLongName,
			&headerModel.CompanyCode, &headerModel.LegalEntity, &headerModel.LegalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract,
			&confirmFormat, &headerModel.Region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&headerModel.Portfolio, &headerModel.UrTrader, &ibPrtPortfolio,
			&ibPortfolio, &ibUrTrader, &headerModel.TzTimeZone, &hasBroker,
			&broker, &optionKey, &pvModel.Pool1, &pvModel.Product1,
			&pvModel.Volume, &fixedPrice, &nonstdFlag, &idxModel.Publication,
			&idxModel.PubIndex, &idxModel.Frequency, &fixPiPbPublication,
			&fixPiPubIndex, &fixFrqFrequency, &headerModel.StartDate, &headerModel.EndDate,
			&pvModel.HolidaySchedule, &headerModel.CreatedBy, &headerModel.CreatedAt, &headerModel.ModifiedBy,
			&headerModel.ModifiedAt, &executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if ibPrtPortfolio.Valid {
			headerModel.IbPrtPortfolio = int(ibPrtPortfolio.Int32)
		}

		if ibPortfolio.Valid {
			headerModel.IbPortfolio = ibPortfolio.String
		}

		if ibUrTrader.Valid {
			headerModel.IbUrTrader = ibUrTrader.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		if optionKey.Valid {
			headerModel.ExercisedOptionKey = int(optionKey.Int32)
		}

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		if fixPiPbPublication.Valid && fixPiPubIndex.Valid && fixFrqFrequency.Valid {
			idxModel2.Publication = fixPiPbPublication.String
			idxModel2.PubIndex = fixPiPubIndex.String
			idxModel2.Frequency = fixFrqFrequency.String
			pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel2)
		}

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate
		pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)

		if nonstdFlag == "N" {
			pvModel.VolSeq = 0
			headerModel.Terms = append(headerModel.Terms, &pvModel)
		} else {
			initialTermModelsMap[headerModel.DealKey] = &pvModel
			headerModelsMap[headerModel.DealKey] = &headerModel
			dealKeys = append(dealKeys, headerModel.DealKey)
		}

		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	if len(dealKeys) > 0 {
		termModelsMap, err := repo.getNucPowerSwapDealTermModel(ctx, dealKeys)
		if err != nil {
			logger.Debugln("error getting getNucPowerSwapDealTermModel: ", err)
			return nil, err
		}

		for dealKey, termModels := range termModelsMap {
			for _, termModel := range termModels {
				termModel.Pool1 = initialTermModelsMap[dealKey].Pool1
				termModel.Product1 = initialTermModelsMap[dealKey].Product1
				termModel.Indexes1 = initialTermModelsMap[dealKey].Indexes1
				termModel.Indexes2 = initialTermModelsMap[dealKey].Indexes2
				termModel.HolidaySchedule = initialTermModelsMap[dealKey].HolidaySchedule

				headerModelsMap[dealKey].Terms = append(headerModelsMap[dealKey].Terms, termModel)
			}
		}
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucPowerOptionsDealByKeys(ctx context.Context, lstPowerOptionkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetNucPowerOptionsDealByKeys")

	dealKeysQuery, params, err := oracle.CreateInQueryFloat64(lstPowerOptionkeys, []interface{}{}, "pd.poption_key")
	if err != nil {
		logger.Debugln("error in CreateInQueryFloat64: ", err)
		return nil, err
	}

	query := getNucPowerOptionsDealByKeysQuery(dealKeysQuery)
	rows, err := repo.nucleusDb.QueryContext(ctx, query, params...)
	if err != nil {
		logger.Debugln("error got when executing getNucPowerOptionsDealByKeysQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var headerModels []*nucleus.NucleusTradeHeaderModel

	formulaRegex, err := regexp.Compile(formulaPattern)
	if err != nil {
		logger.Debugln("error when compiling regex: ", err)
		return nil, err
	}

	for rows.Next() {
		var headerModel nucleus.NucleusTradeHeaderModel
		var pvModel nucleus.NucleusTradeTermModel

		var company, companyLongName, companyCode, legalEntity, legalEntityLongName,
			contract, confirmFormat, hsHedgeKey, hasBroker, broker, executionDate,
			executionTime, exoticFlag, interaffiliateFlag, region, portfolio, ibPortfolio,
			ibUrTrader, settleFormula, strikeFormula sql.NullString
		var ibPrtPortfolio sql.NullInt32
		var totalQuantity, fixedPrice sql.NullFloat64

		if err := rows.Scan(
			&headerModel.DealKey, &headerModel.DealType, &totalQuantity, &headerModel.DnDirection,
			&headerModel.TransactionDate, &headerModel.CyCompanyKey, &company, &companyLongName,
			&companyCode, &legalEntity, &legalEntityLongName,
			&headerModel.CyLegalEntityKey, &interaffiliateFlag, &contract,
			&confirmFormat, &region, &hsHedgeKey, &headerModel.PrtPortfolio,
			&portfolio, &headerModel.UrTrader, &ibPrtPortfolio, &ibPortfolio,
			&ibUrTrader, &headerModel.TzTimeZone, &headerModel.TzExerciseZone, &hasBroker,
			&broker, &pvModel.Pool1, &pvModel.Product1, &pvModel.PointCode1,
			&settleFormula, &headerModel.StartDate, &headerModel.EndDate, &pvModel.HolidaySchedule,
			&pvModel.Volume, &fixedPrice, &pvModel.PriceType, &strikeFormula,
			&headerModel.CreatedBy, &headerModel.ModifiedBy, &headerModel.CreatedAt, &headerModel.ModifiedAt,
			&executionDate, &executionTime, &exoticFlag,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if totalQuantity.Valid {
			headerModel.TotalQuantity = totalQuantity.Float64
		}

		if company.Valid {
			headerModel.Company = company.String
		}

		if companyLongName.Valid {
			headerModel.CompanyLongName = companyLongName.String
		}

		if companyCode.Valid {
			headerModel.CompanyCode = companyCode.String
		}

		if legalEntity.Valid {
			headerModel.LegalEntity = legalEntity.String
		}

		if legalEntityLongName.Valid {
			headerModel.LegalEntityLongName = legalEntityLongName.String
		}

		if interaffiliateFlag.Valid {
			headerModel.InteraffiliateFlag = interaffiliateFlag.String
		}

		if contract.Valid {
			headerModel.Contract = contract.String
		}

		if confirmFormat.Valid {
			headerModel.ConfirmFormat = confirmFormat.String
		}

		if region.Valid {
			headerModel.Region = region.String
		}

		if hsHedgeKey.Valid {
			headerModel.HsHedgeKey = hsHedgeKey.String
		}

		if portfolio.Valid {
			headerModel.Portfolio = portfolio.String
		}

		if ibPrtPortfolio.Valid {
			headerModel.IbPrtPortfolio = int(ibPrtPortfolio.Int32)
		}

		if ibPortfolio.Valid {
			headerModel.IbPortfolio = ibPortfolio.String
		}

		if ibUrTrader.Valid {
			headerModel.IbUrTrader = ibUrTrader.String
		}

		if hasBroker.Valid {
			headerModel.HasBroker = hasBroker.String
		}

		if broker.Valid {
			headerModel.Broker = broker.String
		}

		pvModel.BegDate = headerModel.StartDate
		pvModel.EndDate = headerModel.EndDate
		pvModel.VolSeq = 0

		if fixedPrice.Valid {
			pvModel.FixedPrice = fixedPrice.Float64
		}

		if exoticFlag.Valid {
			headerModel.ExoticFlag = exoticFlag.String
		}

		if executionDate.Valid && executionTime.Valid && executionDate.String != "" && executionTime.String != "" {
			execTime, err := parseExecutionDateTime(executionDate.String, executionTime.String)
			if err != nil {
				logger.Debugln("error parsing executionDate and executionTime: ", err)
				return nil, err
			}
			headerModel.ExecutionTime = execTime
		}

		if settleFormula.Valid && settleFormula.String != "" {
			formula := formulaRegex.FindString(settleFormula.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes1 = append(pvModel.Indexes1, &idxModel)
			}
		}

		if strikeFormula.Valid && strikeFormula.String != "" {
			formula := formulaRegex.FindString(strikeFormula.String)
			formula = strings.ReplaceAll(formula, "[", "")
			formula = strings.ReplaceAll(formula, "]", "")
			formulaSections := strings.Split(formula, "|")

			if len(formulaSections) == 3 {
				var idxModel nucleus.NucleusTradeIndexModel
				idxModel.Publication = formulaSections[0]
				idxModel.PubIndex = formulaSections[1]
				idxModel.Frequency = formulaSections[2]
				pvModel.Indexes2 = append(pvModel.Indexes2, &idxModel)
			}
		}

		headerModel.Terms = append(headerModel.Terms, &pvModel)
		headerModels = append(headerModels, &headerModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return headerModels, nil
}

func (repo *NucleusTradeRepository) GetNucCapacityDealByKeys(ctx context.Context, lstCapacitykeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucPTPDealByKeys(ctx context.Context, lstPTPkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucEmissionDealByKeys(ctx context.Context, lstEmissionkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucEmissionOptionDealByKeys(ctx context.Context, lstEmissionkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucSpreadOptionsDealByKeys(ctx context.Context, lstSpreadOptionkeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucHeatRateSwapsDealByKeys(ctx context.Context, lstHeatRateSwapskeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucTransmissionDealByKeys(ctx context.Context, lstTranskeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucTCCFTRSDealByKeys(ctx context.Context, lstTccFtrskeys []float64, strDealType string) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (repo *NucleusTradeRepository) GetNucMiscChargeDealByKeys(ctx context.Context, lstTranskeys []float64) ([]*nucleus.NucleusTradeHeaderModel, error) {
	return nil, fmt.Errorf("method not implemented")
}

type nucleusProcessedTradeType struct {
	TradeId             int64
	DealType            string
	PortfolioId         int64
	TransactionDate     time.Time
	TradeDetail         string
	AnomalyDetectedFlag bool
	AnomalyTestResult   sql.NullString
	ModelParameters     sql.NullString
}

func (repo *NucleusTradeRepository) ProcessTrades(ctx context.Context, trades []*nucleus.NucleusTradeHeaderModel, anomalyMessages map[int][]common.IModelBasePayload) error {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "ProcessTrades")

	if len(trades) == 0 {
		return nil
	}

	var nucleusProcessedTradeTypeData []nucleusProcessedTradeType

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
						logger.Debugln("error marshalling ModelParams: ", err)
						return err
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
			logger.Debugln("error marshalling trade: ", err)
			return err
		}

		nucleusProcessedTrade := nucleusProcessedTradeType{
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
			nucleusProcessedTrade.ModelParameters = sql.NullString{
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

	if _, err := repo.machineLearningDb.Exec(execProcessTradesQuery, sql.Named("TVP", tvpType)); err != nil {
		logger.Debugln("error in execProcessTradesQuery: ", err)
		return err
	}

	return nil
}

func (repo *NucleusTradeRepository) GetLastExtractionRun(ctx context.Context, tradeDate time.Time, dealType string) (*nucleus.NucleusTradeExtractionRunModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetLastExtractionRun")

	var extractionRun nucleus.NucleusTradeExtractionRunModel
	if err := repo.machineLearningDb.QueryRowContext(ctx, getLastExtractionRunQuery, sql.Named("transactionDate", tradeDate), sql.Named("dealType", dealType)).
		Scan(
			&extractionRun.ExtractionRunId, &extractionRun.TransactionDate, &extractionRun.DealType,
			&extractionRun.TimeParameter, &extractionRun.CreatedAt,
		); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		logger.Debugln("error got when executing getLastExtractionRunQuery: ", err)
		return nil, err
	}

	return &extractionRun, nil
}

func (repo *NucleusTradeRepository) InsertExtractionRun(ctx context.Context, tradeDate time.Time, lastRun time.Time, dealType string) error {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "InsertExtractionRun")

	if _, err := repo.machineLearningDb.ExecContext(ctx, insertExtractionRunQuery,
		sql.Named("transactionDate", tradeDate),
		sql.Named("dealType", dealType),
		sql.Named("timeParameter", lastRun),
	); err != nil {
		logger.Debugln("error got when executing insertExtractionRun: ", err)
		return err
	}

	return nil
}

func (repo *NucleusTradeRepository) GetPortfolioRiskMappingList(ctx context.Context) ([]*nucleus.PortfolioRiskMappingModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetPortfolioRiskMappingList")

	rows, err := repo.machineLearningDb.QueryContext(ctx, getPortfolioRiskMappingListQuery)
	if err != nil {
		logger.Debugln("error got when executing getPortfolioRiskMappingListQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var portfolioRiskModels []*nucleus.PortfolioRiskMappingModel

	for rows.Next() {
		var portfolioRiskModel nucleus.PortfolioRiskMappingModel

		var legalEntity sql.NullString

		if err := rows.Scan(
			&portfolioRiskModel.SourceSystem, &portfolioRiskModel.Portfolio, &legalEntity,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if legalEntity.Valid {
			portfolioRiskModel.LegalEntity = legalEntity.String
		}

		portfolioRiskModels = append(portfolioRiskModels, &portfolioRiskModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return portfolioRiskModels, nil
}

func (repo *NucleusTradeRepository) GetLarBaselist(ctx context.Context) ([]*common.LarBaseModel, error) {
	logger := repo.logger.GetLogger()
	logger = logger.WithField("method", "GetLarBaselist")

	rows, err := repo.machineLearningDb.QueryContext(ctx, getLarBaselistQuery)
	if err != nil {
		logger.Debugln("error got when executing getLarBaselistQuery: ", err)
		return nil, err
	}
	defer rows.Close()

	var larBaseModels []*common.LarBaseModel

	for rows.Next() {
		var larBaseModel common.LarBaseModel

		var shortName, counterpartyLongName, parentCompany, product, sourceSystem, dealType,
			nettingAgreement, agreementTypePerCSA, buyTenor, sellTenor, limitAvailability,
			sPRating, moodyRating, finalInternalRating, finalRating, equifax, amendedBy,
			doddFrankClassification, tradingEntity, legalEntity sql.NullString
		var grossExposure, collateral, netPosition sql.NullFloat64
		var effectiveDate, reviewDate, reportCreatedDate, reportingDate, createdAt sql.NullTime

		if err := rows.Scan(
			&shortName, &counterpartyLongName, &parentCompany, &product, &sourceSystem,
			&dealType, &nettingAgreement, &agreementTypePerCSA, &larBaseModel.OurThreshold,
			&larBaseModel.CounterpartyThreshold, &buyTenor, &sellTenor, &grossExposure,
			&collateral, &netPosition, &larBaseModel.LimitValue, &larBaseModel.LimitCurrency,
			&limitAvailability, &larBaseModel.ExposureLimit, &larBaseModel.ExpirationDate,
			&larBaseModel.MarketType, &larBaseModel.IndustryCode, &sPRating, &moodyRating,
			&finalInternalRating, &finalRating, &equifax, &amendedBy, &effectiveDate,
			&reviewDate, &doddFrankClassification, &reportCreatedDate, &larBaseModel.Boost,
			&tradingEntity, &legalEntity, &larBaseModel.Agmt, &larBaseModel.CSA,
			&larBaseModel.Tenor, &larBaseModel.Limit, &reportingDate, &createdAt,
		); err != nil {
			logger.Debugln("error scanning rows: ", err)
			return nil, err
		}

		if shortName.Valid {
			larBaseModel.ShortName = shortName.String
		}

		if counterpartyLongName.Valid {
			larBaseModel.CounterpartyLongName = counterpartyLongName.String
		}

		if parentCompany.Valid {
			larBaseModel.ParentCompany = parentCompany.String
		}

		if product.Valid {
			larBaseModel.Product = product.String
		}

		if sourceSystem.Valid {
			larBaseModel.SourceSystem = sourceSystem.String
		}

		if dealType.Valid {
			larBaseModel.DealType = dealType.String
		}

		if nettingAgreement.Valid {
			larBaseModel.NettingAgreement = nettingAgreement.String
		}

		if agreementTypePerCSA.Valid {
			larBaseModel.AgreementTypePerCSA = agreementTypePerCSA.String
		}

		if buyTenor.Valid {
			larBaseModel.BuyTenor = buyTenor.String
		}

		if sellTenor.Valid {
			larBaseModel.SellTenor = sellTenor.String
		}

		if grossExposure.Valid {
			larBaseModel.GrossExposure = grossExposure.Float64
		}

		if collateral.Valid {
			larBaseModel.Collateral = collateral.Float64
		}

		if netPosition.Valid {
			larBaseModel.NetPosition = netPosition.Float64
		}

		if limitAvailability.Valid {
			larBaseModel.LimitAvailability = limitAvailability.String
		}

		if sPRating.Valid {
			larBaseModel.SPRating = sPRating.String
		}

		if moodyRating.Valid {
			larBaseModel.MoodyRating = moodyRating.String
		}

		if finalInternalRating.Valid {
			larBaseModel.FinalInternalRating = finalInternalRating.String
		}

		if finalRating.Valid {
			larBaseModel.FinalRating = finalRating.String
		}

		if equifax.Valid {
			larBaseModel.Equifax = equifax.String
		}

		if amendedBy.Valid {
			larBaseModel.AmendedBy = amendedBy.String
		}

		if effectiveDate.Valid {
			larBaseModel.EffectiveDate = effectiveDate.Time
		}

		if reviewDate.Valid {
			larBaseModel.ReviewDate = reviewDate.Time
		}

		if doddFrankClassification.Valid {
			larBaseModel.DoddFrankClassification = doddFrankClassification.String
		}

		if reportCreatedDate.Valid {
			larBaseModel.ReportCreatedDate = reportCreatedDate.Time
		}

		if tradingEntity.Valid {
			larBaseModel.TradingEntity = tradingEntity.String
		}

		if legalEntity.Valid {
			larBaseModel.LegalEntity = legalEntity.String
		}

		if reportingDate.Valid {
			larBaseModel.ReportingDate = reportingDate.Time
		}

		if createdAt.Valid {
			larBaseModel.CreatedAt = createdAt.Time
		}

		larBaseModels = append(larBaseModels, &larBaseModel)
	}

	if closeErr := rows.Close(); closeErr != nil {
		logger.Debugln("error closing the rows: ", err)
		return nil, err
	}

	if err := rows.Err(); err != nil {
		logger.Debugln("error in rows: ", err)
		return nil, err
	}

	return larBaseModels, nil
}
