package main

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
)

func TestToDecimalParsesFormattedAmounts(t *testing.T) {
	tests := map[string]string{
		"1234567.89":             "1234567.89",
		"1 234 567,89":           "1234567.89",
		"1\u00a0234\u00a0567,89": "1234567.89",
		"1.234.567,89":           "1234567.89",
		"1,234,567.89":           "1234567.89",
		"1 234 567":              "1234567",
		"1.234.567":              "1234567",
		"1,234,567":              "1234567",
		"1234567,89 тг":          "1234567.89",
		"-1 234,50":              "-1234.5",
		"99,274":                 "99274",
		"213,389":                "213389",
		"5.6888806E+06":          "5688880.6",
		"5,6888806E+06":          "5688880.6",
		"1.0665008e+007":         "10665008",
	}

	for input, want := range tests {
		got := toDecimal(input).String()
		if got != want {
			t.Fatalf("toDecimal(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCollectFinalLoadsUsesFinalAttachColumn(t *testing.T) {
	workbook := excelize.NewFile()
	sheet := workbook.GetSheetName(0)
	headers := []any{"РП", "Заказчик", "ФИО", "ИИН", "Общая задолженность", "Открепить", "Статус", "Закрепить"}
	for col, value := range headers {
		if err := setCell(workbook, sheet, 1, col+1, value); err != nil {
			t.Fatal(err)
		}
	}
	rows := [][]any{
		{"РП1", "З", "А", "111", "5.6888806E+06", "OLD_LOGIN", "Связь с должником", "NEW_LOGIN"},
		{"РП1", "З", "Б", "222", "1 234 567,89", "OLD_LOGIN", "Оплата по соглашению", "NEW_LOGIN"},
	}
	for rowIndex, row := range rows {
		for col, value := range row {
			if err := setCell(workbook, sheet, rowIndex+2, col+1, value); err != nil {
				t.Fatal(err)
			}
		}
	}

	header, err := readHeader(workbook, sheet)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	loads, _ := collectFinalLoads(workbook, sheet, cols)
	got := loads[loginKey{rp: "РП1", login: "NEW_LOGIN"}]
	if got == nil {
		t.Fatal("missing final login load")
	}
	if got.count != 2 {
		t.Fatalf("count = %d, want 2", got.count)
	}
	if got.amount.String() != "6923448.49" {
		t.Fatalf("amount = %s, want 6923448.49", got.amount.String())
	}
	if got.iinCount != 2 {
		t.Fatalf("iinCount = %d, want 2", got.iinCount)
	}
}

func TestReplaceSummarySheetWritesAmountWithoutFixedDecimalPlaces(t *testing.T) {
	workbook := excelize.NewFile()
	amount := decimal.RequireFromString("1234.56")
	loads := map[loginKey]*load{
		{rp: "РП1", login: "LOGIN1"}: {
			count:    1,
			amount:   amount,
			iinCount: 1,
		},
	}

	if err := replaceSummarySheet(workbook, loads, 0, 0, "Итоги"); err != nil {
		t.Fatal(err)
	}

	got, err := workbook.GetCellValue("Итоги", "D2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "1234.56" {
		t.Fatalf("D2 = %q, want %q", got, "1234.56")
	}
}
