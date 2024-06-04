package entity

// 本行卡
type SzjyLstDtlBat struct {
	Index       string  `json:"-"`
	BalAmt      float32 `json:"bal_amt"`
	CcyCod      string  `json:"ccy_cod"`
	ChnCod      string  `json:"chn_cod"`
	ChnNam      string  `json:"chn_nam"`
	DatDte      string  `json:"dat_dte"`
	DatFlg1     string  `json:"dat_flg1"`
	DatFlg2     string  `json:"dat_flg2"`
	DatFlg3     string  `json:"dat_flg3"`
	DatMon      string  `json:"dat_mon"`
	Db1Cod      string  `json:"db1_cod"`
	DtlTyp      string  `json:"dtl_typ"`
	EacId       string  `json:"eac_id"`
	F1          string  `json:"f1"`
	F2          string  `json:"f2"`
	F3          string  `json:"f3"`
	F4          string  `json:"f4"`
	F5          string  `json:"f5"`
	F6          string  `json:"f6"`
	F7          string  `json:"f7"`
	F8          string  `json:"f8"`
	F9          string  `json:"f9"`
	F10         string  `json:"f10"`
	FileFlag    string  `json:"file_flag"`
	FuzzySearch string  `json:"fuzzy_search"`
	MchCod      string  `json:"mch_cod"`
	Mch2Cod     string  `json:"mch2_cod"`
	PayEac      string  `json:"pay_eac"`
	Picture     string  `json:"picture"`
	RcvEac      string  `json:"rcv_eac"`
	RmbAmt      float64 `json:"rmb_amt"`
	TrxAmt      float64 `json:"trx_amt"`
	TrxCod1     int32   `json:"trx_cod1"`
	TrxCod2     int32   `json:"trx_cod2"`
	TrxCod3     string  `json:"trx_cod3"`
	TrxCrdNbr   string  `json:"trx_crd_nbr"`
	TrxDte      string  `json:"trx_dte"`
	TrxTim      string  `json:"trx_tim"`
}

// 数字钱包
type SzjyWcrdBat struct {
	BalAmt      float32 `json:"bal_amt"`
	CcyCod      string  `json:"ccy_cod"`
	ChnCod      string  `json:"chn_cod"`
	ChnNam      string  `json:"chn_nam"`
	DatDte      string  `json:"dat_dte"`
	DatFlg1     string  `json:"dat_flg1"`
	DatFlg2     string  `json:"dat_flg2"`
	DatFlg3     string  `json:"dat_flg3"`
	DatMon      string  `json:"dat_mon"`
	Db1Cod      string  `json:"db1_cod"`
	Db1Typ      string  `json:"db1_typ"`
	EacId       string  `json:"eac_id"`
	F1          string  `json:"f1"`
	F2          string  `json:"f2"`
	F3          string  `json:"f3"`
	F4          string  `json:"f4"`
	F5          string  `json:"f5"`
	F6          string  `json:"f6"`
	F7          string  `json:"f7"`
	F8          string  `json:"f8"`
	F9          string  `json:"f9"`
	F10         string  `json:"f10"`
	FileFlag    string  `json:"file_flag"`
	FuzzySearch string  `json:"fuzzy_search"`
	MchCod      string  `json:"mch_cod"`
	Mch2Cod     string  `json:"mch2_cod"`
	PayEac      string  `json:"pay_eac"`
	Picture     string  `json:"picture"`
	RcvEac      string  `json:"rcv_eac"`
	RmbAmt      float32 `json:"rmb_amt"`
	TrxAmt      float32 `json:"trx_amt"`
	TrxCod1     float32 `json:"trx_cod1"`
	TrxCod2     float32 `json:"trx_cod2"`
	TrxCod3     float32 `json:"trx_cod3"`
	TrxCrdNbr   string  `json:"trx_crd_nbr"`
	TrxDte      string  `json:"trx_dte"`
	TrxTim      string  `json:"trx_tim"`
}
