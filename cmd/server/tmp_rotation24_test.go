package main

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
)

func TestTmpRotation24Summary(t *testing.T) {
	content, err := os.ReadFile("../../rotation_result (24).xlsx")
	if err != nil {
		t.Fatal(err)
	}
	app := &server{
		hub:     &progressHub{clients: make(map[string]map[chan payload]bool)},
		jobs:    make(map[string]*jobResult),
		dataDir: t.TempDir(),
	}
	output, err := redistributeWorkbook(context.Background(), bytes.NewReader(content), "tmp-24", app, workbookConfig{
		fixedStatuses: rotationFixedStatuses,
		sourceColumn:  "detach",
		strategy:      "full",
		processName:   "ротации",
		summaryTitle:  "Итоги ротации",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/tmp/rotation_result_24_fixed.xlsx", output, 0644); err != nil {
		t.Fatal(err)
	}

	workbook, err := excelize.OpenReader(bytes.NewReader(output))
	if err != nil {
		t.Fatal(err)
	}
	defer workbook.Close()

	mainTotal := sumSourceTotal(t, workbook)
	summaryTotal := sumSummaryTotal(t, workbook, "Итоги ротации")
	logParsedAmountsByStatus(t, workbook)
	t.Logf("main total: %s", mainTotal.StringFixed(2))
	t.Logf("summary total: %s", summaryTotal.StringFixed(2))
	if !mainTotal.Equal(summaryTotal) {
		t.Fatalf("summary total mismatch: main=%s summary=%s", mainTotal.StringFixed(2), summaryTotal.StringFixed(2))
	}
}

func logParsedAmountsByStatus(t *testing.T, workbook *excelize.File) {
	t.Helper()
	rows, err := workbook.GetRows(workbook.GetSheetName(0))
	if err != nil {
		t.Fatal(err)
	}
	header, err := readHeaderFromRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	type item struct {
		count  int
		amount decimal.Decimal
	}
	items := make(map[string]item)
	for _, row := range rows[1:] {
		status := normalizeStatus(getRowCell(row, cols.status))
		current := items[status]
		current.count++
		current.amount = current.amount.Add(toDecimal(getRowCell(row, cols.amount)))
		if status == "полное погашение" || status == "оплата без соглашения" {
			t.Logf("raw amount status=%q value=%q parsed=%s", status, getRowCell(row, cols.amount), toDecimal(getRowCell(row, cols.amount)).String())
		}
		items[status] = current
	}
	for status, item := range items {
		t.Logf("status %q count=%d amount=%s", status, item.count, item.amount.StringFixed(2))
	}
}

func sumSourceTotal(t *testing.T, workbook *excelize.File) decimal.Decimal {
	t.Helper()
	rows, err := workbook.GetRows(workbook.GetSheetName(0))
	if err != nil {
		t.Fatal(err)
	}
	header, err := readHeaderFromRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := findColumns(header)
	if err != nil {
		t.Fatal(err)
	}
	total := decimal.Zero
	for _, row := range rows[1:] {
		if normalizeRP(getRowCell(row, cols.rp)) == "" || normalizeLogin(getRowCell(row, cols.attach)) == "" {
			continue
		}
		total = total.Add(toDecimal(getRowCell(row, cols.amount)))
	}
	return total
}

func sumSummaryTotal(t *testing.T, workbook *excelize.File, sheet string) decimal.Decimal {
	t.Helper()
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		t.Fatal(err)
	}
	total := decimal.Zero
	for _, row := range rows[1:] {
		if normalizeRP(getRowCell(row, 1)) == "" || normalizeLogin(getRowCell(row, 2)) == "" {
			continue
		}
		total = total.Add(toDecimal(getRowCell(row, 4)))
	}
	return total
}
