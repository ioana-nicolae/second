package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/sede-x/RogerRogerAnomalyDetector/handlers"
	models "github.com/sede-x/RogerRogerAnomalyDetector/handlers/models"
	"github.com/sede-x/RogerRogerAnomalyDetector/logger"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/common"
	"github.com/sede-x/RogerRogerAnomalyDetector/models/nucleus"
	"github.com/sede-x/RogerRogerAnomalyDetector/repository/nucleus/power"
)

const (
	// NucleusTradeRepoErrorCode is the error code for
	// fetching data from the database
	NucleusTradeRepoErrorCode = 1000
	// GetNucPowerDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucPowerDealListTradeDateRequiredErrorCode = 1001
	// GetNucPowerDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucPowerDealListDateTimeFormatErrorCode = 1002
	// GetNucPowerSwapDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucPowerSwapDealListTradeDateRequiredErrorCode = 1003
	// GetNucPowerSwapDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucPowerSwapDealListDateTimeFormatErrorCode = 1004
	// GetNucPowerOptionsDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucPowerOptionsDealListTradeDateRequiredErrorCode = 1005
	// GetNucPowerOptionsDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucPowerOptionsDealListDateTimeFormatErrorCode = 1006
	// GetNucCapacityDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucCapacityDealListTradeDateRequiredErrorCode = 1007
	// GetNucCapacityDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucCapacityDealListDateTimeFormatErrorCode = 1008
	// GetNucPTPDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucPTPDealListTradeDateRequiredErrorCode = 1009
	// GetNucPTPDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucPTPDealListDateTimeFormatErrorCode = 1010
	// GetNucEmissionDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucEmissionDealListTradeDateRequiredErrorCode = 1011
	// GetNucEmissionDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucEmissionDealListDateTimeFormatErrorCode = 1012
	// GetNucEmissionOptionDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucEmissionOptionDealListTradeDateRequiredErrorCode = 1013
	// GetNucEmissionOptionDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucEmissionOptionDealListDateTimeFormatErrorCode = 1014
	// GetNucSpreadOptionsDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucSpreadOptionsDealListTradeDateRequiredErrorCode = 1015
	// GetNucSpreadOptionsDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucSpreadOptionsDealListDateTimeFormatErrorCode = 1016
	// GetNucHeatRateSwapsDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucHeatRateSwapsDealListTradeDateRequiredErrorCode = 1017
	// GetNucHeatRateSwapsDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucHeatRateSwapsDealListDateTimeFormatErrorCode = 1018
	// GetNucTCCFTRSDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucTCCFTRSDealListTradeDateRequiredErrorCode = 1019
	// GetNucTCCFTRSDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucTCCFTRSDealListDateTimeFormatErrorCode = 1020
	// GetNucTCCFTRSDealListDealTypeFormatErrorCode is the error code for
	// when the specified deal type is not valid
	GetNucTCCFTRSDealListDealTypeFormatErrorCode = 1021
	// GetNucTransmissionDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucTransmissionDealListTradeDateRequiredErrorCode = 1022
	// GetNucTransmissionDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucTransmissionDealListDateTimeFormatErrorCode = 1023
	// GetNucMiscChargeDealListTradeDateRequiredErrorCode is the error code for
	// when the date time is not present
	GetNucMiscChargeDealListTradeDateRequiredErrorCode = 1024
	// GetNucMiscChargeDealListDateTimeFormatErrorCode is the error code for
	// when the specified date time is not in RFC3339 format
	GetNucMiscChargeDealListDateTimeFormatErrorCode = 1025
	// GetNucPowerDealByKeysRequiredErrorCode is the error code for
	// when the keys aren't present
	GetNucPowerDealByKeysRequiredErrorCode = 1026
	// GetNucPowerDealByKeysFormatErrorCode is the error code for
	// when the keys aren't float64
	GetNucPowerDealByKeysFormatErrorCode = 1027
	// GetNucPowerSwapDealByKeysRequiredErrorCode is the error code for
	// when the keys aren't present
	GetNucPowerSwapDealByKeysRequiredErrorCode = 1028
	// GetNucPowerSwapDealByKeysFormatErrorCode is the error code for
	// when the keys aren't float64
	GetNucPowerSwapDealByKeysFormatErrorCode = 1029
	// GetNucPowerOptionsDealByKeysRequiredErrorCode is the error code for
	// when the keys aren't present
	GetNucPowerOptionsDealByKeysRequiredErrorCode = 1030
	// GetNucPowerOptionsDealByKeysFormatErrorCode is the error code for
	// when the keys aren't float64
	GetNucPowerOptionsDealByKeysFormatErrorCode = 1031
)

func AddNucleusHandlers(router *mux.Router, middleware func(handler http.HandlerFunc) http.Handler, logger logger.Logger, repository power.INucleusTradeRepository) {
	getNucPowerDealListHandler := http.HandlerFunc(makeGetNucPowerDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerDealList/{lastRunTime}/{tradeDate}", middleware(getNucPowerDealListHandler)).Methods("GET")

	getNucPowerSwapDealListHandler := http.HandlerFunc(makeGetNucPowerSwapDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerSwapDealList/{lastRunTime}/{tradeDate}", middleware(getNucPowerSwapDealListHandler)).Methods("GET")

	getNucPowerOptionsDealListHandler := http.HandlerFunc(makeGetNucPowerOptionsDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerOptionsDealList/{lastRunTime}/{tradeDate}", middleware(getNucPowerOptionsDealListHandler)).Methods("GET")

	getNucCapacityDealListHandler := http.HandlerFunc(makeGetNucCapacityDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucCapacityDealList/{lastRunTime}/{tradeDate}", middleware(getNucCapacityDealListHandler)).Methods("GET")

	getNucPTPDealListHandler := http.HandlerFunc(makeGetNucPTPDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPTPDealList/{lastRunTime}/{tradeDate}", middleware(getNucPTPDealListHandler)).Methods("GET")

	getNucEmissionDealListHandler := http.HandlerFunc(makeGetNucEmissionDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucEmissionDealList/{lastRunTime}/{tradeDate}", middleware(getNucEmissionDealListHandler)).Methods("GET")

	getNucEmissionOptionDealListHandler := http.HandlerFunc(makeGetNucEmissionOptionDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucEmissionOptionDealList/{lastRunTime}/{tradeDate}", middleware(getNucEmissionOptionDealListHandler)).Methods("GET")

	getNucSpreadOptionsDealListHandler := http.HandlerFunc(makeGetNucSpreadOptionsDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucSpreadOptionsDealList/{lastRunTime}/{tradeDate}", middleware(getNucSpreadOptionsDealListHandler)).Methods("GET")

	getNucHeatRateSwapsDealListHandler := http.HandlerFunc(makeGetNucHeatRateSwapsDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucHeatRateSwapsDealList/{lastRunTime}/{tradeDate}", middleware(getNucHeatRateSwapsDealListHandler)).Methods("GET")

	// dealType: FTROPT, FTRSWP, TCCSWP
	getNucTCCFTRSDealListHandler := http.HandlerFunc(makeGetNucTCCFTRSDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucTCCFTRSDealList/{lastRunTime}/{tradeDate}/{dealType}", middleware(getNucTCCFTRSDealListHandler)).Methods("GET")

	getNucTransmissionDealListHandler := http.HandlerFunc(makeGetNucTransmissionDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucTransmissionDealList/{lastRunTime}/{tradeDate}", middleware(getNucTransmissionDealListHandler)).Methods("GET")

	getNucMiscChargeDealListHandler := http.HandlerFunc(makeGetNucMiscChargeDealListHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucMiscChargeDealList/{lastRunTime}/{tradeDate}", middleware(getNucMiscChargeDealListHandler)).Methods("GET")

	processTradesHandler := http.HandlerFunc(makeProcessTradesHandlerNucleus(logger, repository))
	router.Handle("/nucleus/power/ProcessTrades", middleware(processTradesHandler)).Methods("POST")

	getLastExtractionRunHandler := http.HandlerFunc(makeGetLastExtractionRunHandlerNucleus(logger, repository))
	router.Handle("/nucleus/power/GetLastExtractionRun/{tradeDate}/{dealType}", middleware(getLastExtractionRunHandler)).Methods("GET")

	insertExtractionRunHandler := http.HandlerFunc(makeInsertExtractionRunHandlerNucleus(logger, repository))
	router.Handle("/nucleus/power/InsertExtractionRun", middleware(insertExtractionRunHandler)).Methods("POST")

	getPortfolioRiskMappingListHandler := http.HandlerFunc(makeGetPortfolioRiskMappingListHandler(logger, repository))
	router.Handle("/nucleus/power/GetPortfolioRiskMappingList", middleware(getPortfolioRiskMappingListHandler)).Methods("GET")

	getLarBaselistHandler := http.HandlerFunc(makeGetLarBaselistHandlerNucleus(logger, repository))
	router.Handle("/nucleus/power/GetLarBaselist", middleware(getLarBaselistHandler)).Methods("GET")

	getNucPowerDealByKeysHandler := http.HandlerFunc(makeGetNucPowerDealByKeysHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerDealByKeys", middleware(getNucPowerDealByKeysHandler)).Methods("GET")

	getNucPowerSwapDealByKeysHandler := http.HandlerFunc(makeGetNucPowerSwapDealByKeysHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerSwapDealByKeys", middleware(getNucPowerSwapDealByKeysHandler)).Methods("GET")

	getNucPowerOptionsDealByKeysHandler := http.HandlerFunc(makeGetNucPowerOptionsDealByKeysHandler(logger, repository))
	router.Handle("/nucleus/power/GetNucPowerOptionsDealByKeys", middleware(getNucPowerOptionsDealByKeysHandler)).Methods("GET")
}

func makeGetNucPowerDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucPowerDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucPowerDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucPowerDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucPowerDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucPowerDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to get Nucleus Power Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPowerSwapDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucPowerSwapDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucPowerSwapDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucPowerSwapDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucPowerSwapDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucPowerSwapDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to get Nucleus Power Swap Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPowerOptionsDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucPowerOptionsDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucPowerOptionsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucPowerOptionsDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucPowerOptionsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucPowerOptionsDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Power Options Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucCapacityDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucCapacityDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucCapacityDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucCapacityDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucCapacityDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucCapacityDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to get Nucleus Capacity Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPTPDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucPTPDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucPTPDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucPTPDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucPTPDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucPTPDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus PTP Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucEmissionDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucEmissionDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucEmissionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucEmissionDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucEmissionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucEmissionDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Emission Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucEmissionOptionDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucEmissionOptionDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucEmissionOptionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucEmissionOptionDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucEmissionOptionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucEmissionOptionDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Emission Option Deal List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucSpreadOptionsDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucSpreadOptionsDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucSpreadOptionsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucSpreadOptionsDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucSpreadOptionsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucSpreadOptionsDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Spread Options DealList",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucHeatRateSwapsDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucHeatRateSwapsDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucHeatRateSwapsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucHeatRateSwapsDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucHeatRateSwapsDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucHeatRateSwapsDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Heat Rate Swaps DealList",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucTCCFTRSDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucTCCFTRSDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucTCCFTRSDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucTCCFTRSDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucTCCFTRSDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		dealType, ok := params["dealType"]
		if !ok || dealType == "" {
			if err := handlers.SendBadRequest(w, r, "dealType is required",
				models.NewServerError(GetNucTCCFTRSDealListDealTypeFormatErrorCode, "dealType is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		results, err := repository.GetNucTCCFTRSDealList(r.Context(), lastRunTime, tradeDate, dealType)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus TCCFTRS DealList",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucTransmissionDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucTransmissionDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucTransmissionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucTransmissionDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucTransmissionDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucTransmissionDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Heat Rate Swaps DealList",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucMiscChargeDealListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		lastRunTimeParam, ok := params["lastRunTime"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "lastRunTime is required",
				models.NewServerError(GetNucMiscChargeDealListTradeDateRequiredErrorCode, "lastRunTime is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		lastRunTime, err := time.Parse(time.RFC3339, lastRunTimeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse lastRunTime as time in RFC3339",
				models.NewServerError(GetNucMiscChargeDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetNucMiscChargeDealListTradeDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetNucMiscChargeDealListDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		results, err := repository.GetNucMiscChargeDealList(r.Context(), lastRunTime, tradeDate)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nucleus Heat Rate Swaps DealList",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

type makeProcessTradesHandlerNucleusBody struct {
	Trades          []*nucleus.NucleusTradeHeaderModel `json:"trades"`
	AnomalyMessages map[int][]processedTradeExample    `json:"anomalyMessages"`
}

func makeProcessTradesHandlerNucleus(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		var data makeProcessTradesHandlerNucleusBody
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse body",
				models.NewServerError(ProcessTradesBodyFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		anomalyMessages := make(map[int][]common.IModelBasePayload)

		for index := range data.AnomalyMessages {
			for _, value := range data.AnomalyMessages[index] {
				// we need to convert the coming value from ModelBasePayload to
				// ResultModelBasePayload, that way we can read all the properties
				// from the incoming json and we can ingnore the necessary values
				// when we save the data to the database
				newValue := processedTradeExampleResult{
					Company:  value.Company,
					Trader:   value.Trader,
					Source:   value.Source,
					Location: value.Location,
					Broker:   value.Broker,
					ResultModelBasePayload: &common.ResultModelBasePayload{
						ModelName:   value.ModelName,
						Message:     value.Message,
						DealKey:     value.DealKey,
						DealType:    value.DealType,
						ScoredLabel: value.ScoredLabel,
					},
				}

				anomalyMessages[index] = append(anomalyMessages[index], newValue)
			}
		}

		if err := repository.ProcessTrades(r.Context(), data.Trades, anomalyMessages); err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Process Trades",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, nil); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetLastExtractionRunHandlerNucleus(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		tradeDateParam, ok := params["tradeDate"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "tradeDate is required",
				models.NewServerError(GetLastExtractionRunDateRequiredErrorCode, "tradeDate is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		tradeDate, err := time.Parse(time.RFC3339, tradeDateParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse tradeDate as time in RFC3339",
				models.NewServerError(GetLastExtractionRunDateTimeFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		dealTypeParam, ok := params["dealType"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "dealType is required",
				models.NewServerError(GetLastExtractionRunDateRequiredErrorCode, "dealType is required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		results, err := repository.GetLastExtractionRun(r.Context(), tradeDate, dealTypeParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Last Extraction Run",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

type insertExtractionRunHandlerNucleusBody struct {
	TradeDate time.Time `json:"tradeDate"`
	DealType  string    `json:"dealType"`
	LastRun   time.Time `json:"lastRun"`
}

func makeInsertExtractionRunHandlerNucleus(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		var data insertExtractionRunHandlerNucleusBody
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendBadRequest(w, r, "failed to parse body",
				models.NewServerError(InsertExtractionRunBodyFormatErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}
		if err := repository.InsertExtractionRun(r.Context(), data.TradeDate, data.LastRun, data.DealType); err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Insert Extraction Run",
				models.NewServerError(EndureGasTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, nil); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetPortfolioRiskMappingListHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		results, err := repository.GetPortfolioRiskMappingList(r.Context())
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Portfolio Risk Mapping List",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetLarBaselistHandlerNucleus(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		results, err := repository.GetLarBaselist(r.Context())
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Lar Baselist",
				models.NewServerError(NucleusTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPowerDealByKeysHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		keysParamQuery, ok := query["keys"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "keys are required",
				models.NewServerError(GetNucPowerDealByKeysRequiredErrorCode, "Keys are required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		var keysParam []float64
		for _, key := range keysParamQuery {
			keyFloat64, err := strconv.ParseFloat(key, 64)
			if err != nil {
				if err := handlers.SendBadRequest(w, r, "keys must be float64",
					models.NewServerError(GetNucPowerDealByKeysFormatErrorCode, "keys must be float64")); err != nil {
					gLogger.Errorln(err)
				}
			}
			keysParam = append(keysParam, keyFloat64)
		}

		results, err := repository.GetNucPowerDealByKeys(r.Context(), keysParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nuc Power Deal By Keys",
				models.NewServerError(EndureGasTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPowerSwapDealByKeysHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		keysParamQuery, ok := query["keys"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "keys are required",
				models.NewServerError(GetNucPowerSwapDealByKeysRequiredErrorCode, "Keys are required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		var keysParam []float64
		for _, key := range keysParamQuery {
			keyFloat64, err := strconv.ParseFloat(key, 64)
			if err != nil {
				if err := handlers.SendBadRequest(w, r, "keys must be float64",
					models.NewServerError(GetNucPowerSwapDealByKeysFormatErrorCode, "keys must be float64")); err != nil {
					gLogger.Errorln(err)
				}
			}
			keysParam = append(keysParam, keyFloat64)
		}

		results, err := repository.GetNucPowerSwapDealByKeys(r.Context(), keysParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nuc Power Swap Deal By Keys",
				models.NewServerError(EndureGasTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}

func makeGetNucPowerOptionsDealByKeysHandler(logger logger.Logger, repository power.INucleusTradeRepository) func(http.ResponseWriter, *http.Request) {
	gLogger := logger.GetLogger()
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		keysParamQuery, ok := query["keys"]
		if !ok {
			if err := handlers.SendBadRequest(w, r, "keys are required",
				models.NewServerError(GetNucPowerOptionsDealByKeysRequiredErrorCode, "Keys are required")); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		var keysParam []float64
		for _, key := range keysParamQuery {
			keyFloat64, err := strconv.ParseFloat(key, 64)
			if err != nil {
				if err := handlers.SendBadRequest(w, r, "keys must be float64",
					models.NewServerError(GetNucPowerOptionsDealByKeysFormatErrorCode, "keys must be float64")); err != nil {
					gLogger.Errorln(err)
				}
			}
			keysParam = append(keysParam, keyFloat64)
		}

		results, err := repository.GetNucPowerOptionsDealByKeys(r.Context(), keysParam)
		if err != nil {
			gLogger.Errorln(err)
			if err := handlers.SendInternalServerError(w, r, "unable to Get Nuc Power Options Deal By Keys",
				models.NewServerError(EndureGasTradeRepoErrorCode, err.Error())); err != nil {
				gLogger.Errorln(err)
			}
			return
		}

		if err := handlers.SendOk(w, results); err != nil {
			gLogger.Errorln(err)
		}
	}
}
