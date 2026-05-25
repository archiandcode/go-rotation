package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/xuri/excelize/v2"
)

var rotationFixedStatuses = map[string]bool{
	"оплата пв":                 true,
	"оплата по соглашению":      true,
	"оплата по первому условию": true,
	"оплата по второму условию": true,
	"полное погашение":          true,
	"полное погошение":          true,
}

var alignmentFixedStatuses = map[string]bool{
	"оплата пв":                 true,
	"оплата по соглашению":      true,
	"оплата по первому условию": true,
	"оплата по второму условию": true,
	"полное погашение":          true,
	"полное погошение":          true,
	"должник обещает оплату":    true,
}

var rotationScoreWeights = scoreWeights{
	amount:        decimal.NewFromInt(1),
	materialCount: decimal.NewFromInt(1),
	iinCount:      decimal.NewFromInt(1),
}
var alignmentScoreWeights = scoreWeights{
	amount:        decimal.NewFromInt(1),
	materialCount: decimal.Zero,
	iinCount:      decimal.NewFromInt(1),
}

func init() {
	decimal.DivisionPrecision = 80
}

type server struct {
	hub      *progressHub
	jobs     map[string]*jobResult
	sessions map[string]time.Time
	password string
	dataDir  string
	mu       sync.Mutex
}

type jobResult struct {
	id          string
	process     string
	state       string
	percent     int
	message     string
	resultFile  string
	filename    string
	err         string
	createdAt   time.Time
	startedAt   time.Time
	completedAt time.Time
	cancel      context.CancelFunc
	log         []progressRecord
}

type progressRecord struct {
	At      time.Time `json:"at"`
	Percent int       `json:"percent"`
	Message string    `json:"message"`
}

type persistedJob struct {
	ID          string           `json:"id"`
	Process     string           `json:"process"`
	State       string           `json:"state"`
	Percent     int              `json:"percent"`
	Message     string           `json:"message"`
	ResultFile  string           `json:"result_file,omitempty"`
	Filename    string           `json:"filename,omitempty"`
	Error       string           `json:"error,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	StartedAt   time.Time        `json:"started_at,omitempty"`
	CompletedAt time.Time        `json:"completed_at,omitempty"`
	Log         []progressRecord `json:"log,omitempty"`
}

type jobView struct {
	ID          string           `json:"id"`
	Process     string           `json:"process"`
	State       string           `json:"state"`
	Percent     int              `json:"percent"`
	Message     string           `json:"message"`
	Filename    string           `json:"filename,omitempty"`
	Error       string           `json:"error,omitempty"`
	DownloadURL string           `json:"download_url,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	StartedAt   time.Time        `json:"started_at,omitempty"`
	CompletedAt time.Time        `json:"completed_at,omitempty"`
	Log         []progressRecord `json:"log,omitempty"`
}

type payload map[string]any

type progressHub struct {
	mu      sync.Mutex
	clients map[string]map[chan payload]bool
}

type columns struct {
	rp, iin, detach, attach, status, amount int
}

type loginKey struct {
	rp    string
	login string
}

type iinGroup struct {
	rp           string
	iin          string
	rows         []int
	amount       decimal.Decimal
	pinnedLogin  string
	currentLogin string
}

type load struct {
	count    int
	amount   decimal.Decimal
	iinCount int
}

type scoreWeights struct {
	amount        decimal.Decimal
	materialCount decimal.Decimal
	iinCount      decimal.Decimal
}

type workbookConfig struct {
	fixedStatuses map[string]bool
	sourceColumn  string
	strategy      string
	processName   string
	summaryTitle  string
}

type progressFunc func(percent int, message string)

type progressCounter struct {
	mu           sync.Mutex
	progress     progressFunc
	startPercent int
	endPercent   int
	done         int
	total        int
	lastPercent  int
	label        string
}

type rpBalanceResult struct {
	rp          string
	assignments map[string]loginKey
	loads       map[loginKey]*load
	loginIINs   map[loginKey]map[string]bool
	err         error
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func main() {
	app := &server{
		hub:      &progressHub{clients: make(map[string]map[chan payload]bool)},
		jobs:     make(map[string]*jobResult),
		sessions: make(map[string]time.Time),
		password: os.Getenv("APP_PASSWORD"),
		dataDir:  os.Getenv("DATA_DIR"),
	}
	if app.password == "" {
		app.password = "admin"
	}
	if app.dataDir == "" {
		app.dataDir = "data"
	}
	if err := app.initStorage(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", app.index)
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
	mux.HandleFunc("/api/me", app.me)
	mux.HandleFunc("/api/login", app.login)
	mux.HandleFunc("/api/logout", app.logout)
	mux.HandleFunc("/api/jobs", app.requireAuth(app.jobsList))
	mux.HandleFunc("/api/jobs/", app.requireAuth(app.jobAction))
	mux.HandleFunc("/api/history/clear", app.requireAuth(app.clearHistory))
	mux.HandleFunc("/rotate/", app.requireAuth(app.rotateFile))
	mux.HandleFunc("/balance/", app.requireAuth(app.balanceFile))
	mux.HandleFunc("/download/", app.requireAuth(app.downloadFile))
	mux.HandleFunc("/ws/rotation/", app.requireAuth(app.websocket))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "static/index.html")
}

func (s *server) historyPath() string {
	return filepath.Join(s.dataDir, "history.json")
}

func (s *server) resultsDir() string {
	return filepath.Join(s.dataDir, "results")
}

func (s *server) initStorage() error {
	if err := os.MkdirAll(s.resultsDir(), 0755); err != nil {
		return err
	}
	content, err := os.ReadFile(s.historyPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var history []persistedJob
	if err := json.Unmarshal(content, &history); err != nil {
		return err
	}
	changed := false
	for _, item := range history {
		job := &jobResult{
			id:          item.ID,
			process:     item.Process,
			state:       item.State,
			percent:     item.Percent,
			message:     item.Message,
			resultFile:  item.ResultFile,
			filename:    item.Filename,
			err:         item.Error,
			createdAt:   item.CreatedAt,
			startedAt:   item.StartedAt,
			completedAt: item.CompletedAt,
			log:         append([]progressRecord(nil), item.Log...),
		}
		if job.state == "running" {
			job.state = "canceled"
			job.message = "Задача прервана при перезапуске сервера."
			job.completedAt = time.Now()
			job.cancel = nil
			changed = true
		}
		s.jobs[job.id] = job
	}
	if changed {
		s.saveHistoryLocked()
	}
	return nil
}

func (s *server) saveHistoryLocked() {
	history := make([]persistedJob, 0, len(s.jobs))
	for _, job := range s.jobs {
		history = append(history, persistedJob{
			ID:          job.id,
			Process:     job.process,
			State:       job.state,
			Percent:     job.percent,
			Message:     job.message,
			ResultFile:  job.resultFile,
			Filename:    job.filename,
			Error:       job.err,
			CreatedAt:   job.createdAt,
			StartedAt:   job.startedAt,
			CompletedAt: job.completedAt,
			Log:         append([]progressRecord(nil), job.log...),
		})
	}
	sort.Slice(history, func(i, j int) bool {
		return history[i].CreatedAt.After(history[j].CreatedAt)
	})
	content, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		log.Printf("save history: %v", err)
		return
	}
	if err := os.WriteFile(s.historyPath(), content, 0644); err != nil {
		log.Printf("save history: %v", err)
	}
}

func (s *server) resultPath(resultFile string) string {
	if resultFile == "" {
		return ""
	}
	return filepath.Join(s.dataDir, filepath.FromSlash(resultFile))
}

func (s *server) me(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	_, ok := s.currentSession(r)
	_ = json.NewEncoder(w).Encode(map[string]bool{"authenticated": ok})
}

func (s *server) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var request struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeJSONError(w, "Введите пароль.", http.StatusBadRequest)
		return
	}
	if subtle.ConstantTimeCompare([]byte(request.Password), []byte(s.password)) != 1 {
		writeJSONError(w, "Неверный пароль.", http.StatusUnauthorized)
		return
	}
	sessionID := newJobID()
	expires := time.Now().Add(24 * time.Hour)
	s.mu.Lock()
	s.sessions[sessionID] = expires
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{
		Name:     "rotation_session",
		Value:    sessionID,
		Path:     "/",
		Expires:  expires,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	_ = json.NewEncoder(w).Encode(map[string]bool{"authenticated": true})
}

func (s *server) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cookie, err := r.Cookie("rotation_session"); err == nil {
		s.mu.Lock()
		delete(s.sessions, cookie.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "rotation_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	_ = json.NewEncoder(w).Encode(map[string]bool{"authenticated": false})
}

func (s *server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.currentSession(r); !ok {
			writeJSONError(w, "Требуется авторизация.", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *server) currentSession(r *http.Request) (string, bool) {
	cookie, err := r.Cookie("rotation_session")
	if err != nil || cookie.Value == "" {
		return "", false
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.sessions[cookie.Value]
	if !ok || now.After(expires) {
		delete(s.sessions, cookie.Value)
		return "", false
	}
	return cookie.Value, true
}

func (s *server) rotateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	file, _, jobID, err := validateUpload(r)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}

	if !s.startJob(content, jobID, "rotation", "rotation_result.xlsx", workbookConfig{
		fixedStatuses: rotationFixedStatuses,
		sourceColumn:  "detach",
		strategy:      "full",
		processName:   "ротации",
		summaryTitle:  "Итоги ротации",
	}) {
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "state": "running"})
}

func (s *server) balanceFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	file, _, jobID, err := validateUpload(r)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		writeJSONError(w, "Не удалось прочитать файл.", http.StatusBadRequest)
		return
	}

	if !s.startJob(content, jobID, "alignment", "alignment_result.xlsx", workbookConfig{
		fixedStatuses: alignmentFixedStatuses,
		sourceColumn:  "attach",
		strategy:      "partial",
		processName:   "выравнивания",
		summaryTitle:  "Итоги выравнивания",
	}) {
		writeJSONError(w, "Дождитесь завершения предыдущей операции или отмените ее.", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "state": "running"})
}

func (s *server) startJob(content []byte, jobID, process, filename string, cfg workbookConfig) bool {
	ctx, cancel := context.WithCancel(context.Background())
	job := &jobResult{
		id:        jobID,
		process:   process,
		state:     "running",
		percent:   0,
		message:   "Задача создана",
		filename:  filename,
		createdAt: time.Now(),
		startedAt: time.Now(),
		cancel:    cancel,
	}
	s.mu.Lock()
	if s.hasRunningJobLocked() {
		s.mu.Unlock()
		cancel()
		return false
	}
	s.jobs[jobID] = job
	s.saveHistoryLocked()
	s.mu.Unlock()

	go s.runJob(ctx, content, jobID, filename, cfg)
	return true
}

func (s *server) hasRunningJobLocked() bool {
	for _, job := range s.jobs {
		if job.state == "running" {
			return true
		}
	}
	return false
}

func (s *server) runJob(ctx context.Context, content []byte, jobID, filename string, cfg workbookConfig) {
	output, err := redistributeWorkbook(ctx, bytes.NewReader(content), jobID, s, cfg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.storeJobCanceled(jobID)
			return
		}
		s.storeJobError(jobID, err.Error())
		return
	}
	if err := ctx.Err(); err != nil {
		s.storeJobCanceled(jobID)
		return
	}

	resultFile := filepath.ToSlash(filepath.Join("results", jobID+".xlsx"))
	if err := os.WriteFile(s.resultPath(resultFile), output, 0644); err != nil {
		s.storeJobError(jobID, "Не удалось сохранить результат.")
		return
	}

	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		job.state = "ready"
		job.resultFile = resultFile
		job.filename = filename
		job.percent = 100
		job.message = "Файл готов."
		job.completedAt = time.Now()
		job.cancel = nil
		s.saveHistoryLocked()
	}
	s.mu.Unlock()
	s.hub.send(jobID, payload{
		"type":         "job_ready",
		"message":      "Файл готов.",
		"download_url": "/download/" + jobID + "/",
	})
}

func (s *server) storeJobError(jobID, message string) {
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		job.state = "error"
		job.err = message
		job.message = message
		job.completedAt = time.Now()
		job.cancel = nil
		s.saveHistoryLocked()
	}
	s.mu.Unlock()
	s.hub.send(jobID, payload{"type": "job_error", "message": message})
}

func (s *server) storeJobCanceled(jobID string) {
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		job.state = "canceled"
		job.message = "Задача отменена."
		job.completedAt = time.Now()
		job.cancel = nil
		s.saveHistoryLocked()
	}
	s.mu.Unlock()
	s.hub.send(jobID, payload{"type": "job_canceled", "message": "Задача отменена."})
}

func (s *server) downloadFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	jobID := strings.TrimPrefix(r.URL.Path, "/download/")
	jobID = strings.TrimSuffix(jobID, "/")
	if jobID == "" {
		http.NotFound(w, r)
		return
	}

	s.mu.Lock()
	result, ok := s.jobs[jobID]
	s.mu.Unlock()
	if !ok {
		writeJSONError(w, "Результат не найден.", http.StatusNotFound)
		return
	}
	if result.state == "running" {
		writeJSONError(w, "Файл еще обрабатывается.", http.StatusConflict)
		return
	}
	if result.state == "error" {
		writeJSONError(w, result.err, http.StatusBadRequest)
		return
	}
	if result.state == "canceled" {
		writeJSONError(w, "Задача отменена.", http.StatusBadRequest)
		return
	}
	content, err := os.ReadFile(s.resultPath(result.resultFile))
	if err != nil {
		writeJSONError(w, "Файл результата не найден.", http.StatusNotFound)
		return
	}
	writeXLSX(w, content, result.filename)
}

func (s *server) jobsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	views := make([]jobView, 0, len(s.jobs))
	for _, job := range s.jobs {
		views = append(views, s.jobViewLocked(job, false))
	}
	s.mu.Unlock()
	sort.Slice(views, func(i, j int) bool {
		return views[i].CreatedAt.After(views[j].CreatedAt)
	})
	_ = json.NewEncoder(w).Encode(views)
}

func (s *server) jobAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	path = strings.Trim(path, "/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(path, "/")
	jobID := parts[0]
	if len(parts) == 2 && parts[1] == "cancel" {
		s.cancelJob(w, r, jobID)
		return
	}
	if len(parts) != 1 || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}

	s.mu.Lock()
	job := s.jobs[jobID]
	if job == nil {
		s.mu.Unlock()
		writeJSONError(w, "Задача не найдена.", http.StatusNotFound)
		return
	}
	view := s.jobViewLocked(job, true)
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(view)
}

func (s *server) cancelJob(w http.ResponseWriter, r *http.Request, jobID string) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	job := s.jobs[jobID]
	if job == nil {
		s.mu.Unlock()
		writeJSONError(w, "Задача не найдена.", http.StatusNotFound)
		return
	}
	if job.state != "running" || job.cancel == nil {
		view := s.jobViewLocked(job, true)
		s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(view)
		return
	}
	cancel := job.cancel
	s.mu.Unlock()
	cancel()
	_ = json.NewEncoder(w).Encode(map[string]string{"state": "canceling"})
}

func (s *server) jobViewLocked(job *jobResult, includeLog bool) jobView {
	view := jobView{
		ID:          job.id,
		Process:     job.process,
		State:       job.state,
		Percent:     job.percent,
		Message:     job.message,
		Filename:    job.filename,
		Error:       job.err,
		CreatedAt:   job.createdAt,
		StartedAt:   job.startedAt,
		CompletedAt: job.completedAt,
	}
	if job.state == "ready" && job.resultFile != "" {
		view.DownloadURL = "/download/" + job.id + "/"
	}
	if includeLog {
		view.Log = append([]progressRecord(nil), job.log...)
	}
	return view
}

func (s *server) clearHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var cancels []context.CancelFunc
	s.mu.Lock()
	for _, job := range s.jobs {
		if job.cancel != nil {
			cancels = append(cancels, job.cancel)
		}
	}
	s.jobs = make(map[string]*jobResult)
	s.saveHistoryLocked()
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	if err := os.RemoveAll(s.resultsDir()); err != nil {
		writeJSONError(w, "Не удалось очистить папку результатов.", http.StatusInternalServerError)
		return
	}
	if err := os.MkdirAll(s.resultsDir(), 0755); err != nil {
		writeJSONError(w, "Не удалось создать папку результатов.", http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]bool{"cleared": true})
}

func (s *server) websocket(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/ws/rotation/")
	jobID = strings.TrimSuffix(jobID, "/")
	if jobID == "" {
		http.NotFound(w, r)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ch := s.hub.add(jobID)
	defer s.hub.remove(jobID, ch)

	s.mu.Lock()
	job := s.jobs[jobID]
	if job != nil {
		_ = conn.WriteJSON(payload{
			"type":    "progress",
			"percent": job.percent,
			"message": job.message,
			"state":   job.state,
		})
		if job.state == "ready" {
			_ = conn.WriteJSON(payload{"type": "job_ready", "message": job.message, "download_url": "/download/" + jobID + "/"})
		}
	} else {
		_ = conn.WriteJSON(payload{
			"type":    "progress",
			"percent": 0,
			"message": "Готов к загрузке файла",
		})
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case msg := <-ch:
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (h *progressHub) add(jobID string) chan payload {
	ch := make(chan payload, 16)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.clients[jobID] == nil {
		h.clients[jobID] = make(map[chan payload]bool)
	}
	h.clients[jobID][ch] = true
	return ch
}

func (h *progressHub) remove(jobID string, ch chan payload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients[jobID], ch)
	close(ch)
	if len(h.clients[jobID]) == 0 {
		delete(h.clients, jobID)
	}
}

func (h *progressHub) send(jobID string, msg payload) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients[jobID] {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (s *server) sendProgress(jobID string, percent int, message string) {
	record := progressRecord{At: time.Now(), Percent: percent, Message: message}
	s.mu.Lock()
	if job := s.jobs[jobID]; job != nil {
		job.percent = percent
		job.message = message
		job.log = append(job.log, record)
		if len(job.log) > 200 {
			job.log = job.log[len(job.log)-200:]
		}
		s.saveHistoryLocked()
	}
	s.mu.Unlock()
	s.hub.send(jobID, payload{
		"type":    "progress",
		"percent": percent,
		"message": message,
	})
}

func reportProgress(progress progressFunc, percent int, message string) {
	if progress != nil {
		progress(percent, message)
	}
}

func reportRangeProgress(progress progressFunc, startPercent, endPercent, done, total, lastPercent int, label string) int {
	if progress == nil || total <= 0 {
		return lastPercent
	}
	percent := startPercent + ((endPercent - startPercent) * done / total)
	if percent <= lastPercent {
		return lastPercent
	}
	if percent > endPercent {
		percent = endPercent
	}
	progress(percent, fmt.Sprintf("%s: %d из %d", label, done, total))
	return percent
}

func newProgressCounter(progress progressFunc, startPercent, endPercent, total int, label string) *progressCounter {
	return &progressCounter{
		progress:     progress,
		startPercent: startPercent,
		endPercent:   endPercent,
		total:        max(total, 1),
		lastPercent:  startPercent,
		label:        label,
	}
}

func (counter *progressCounter) add(done int) {
	if counter == nil || counter.progress == nil || done <= 0 {
		return
	}
	counter.mu.Lock()
	defer counter.mu.Unlock()
	counter.done += done
	counter.lastPercent = reportRangeProgress(
		counter.progress,
		counter.startPercent,
		counter.endPercent,
		counter.done,
		counter.total,
		counter.lastPercent,
		counter.label,
	)
}

func validateUpload(r *http.Request) (io.ReadCloser, string, string, error) {
	if err := r.ParseMultipartForm(64 << 20); err != nil {
		return nil, "", "", errors.New("Загрузите XLSX файл.")
	}
	jobID := r.FormValue("job_id")
	if jobID == "" {
		jobID = newJobID()
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		return nil, "", jobID, errors.New("Загрузите XLSX файл.")
	}
	if !strings.HasSuffix(strings.ToLower(header.Filename), ".xlsx") {
		_ = file.Close()
		return nil, "", jobID, errors.New("Поддерживается только формат .xlsx.")
	}
	return file, header.Filename, jobID, nil
}

func newJobID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

func writeJSONError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeXLSX(w http.ResponseWriter, content []byte, filename string) {
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	_, _ = w.Write(content)
}

func redistributeWorkbook(ctx context.Context, input io.Reader, jobID string, app *server, cfg workbookConfig) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 5, "Файл загружен, читаю Excel")

	workbook, err := excelize.OpenReader(input)
	if err != nil {
		return nil, errors.New("Не удалось прочитать Excel файл.")
	}
	defer workbook.Close()

	sheet := workbook.GetSheetName(0)
	if sheet == "" {
		return nil, errors.New("Не найден первый лист Excel.")
	}

	rows, err := workbook.GetRows(sheet)
	if err != nil {
		return nil, errors.New("Не удалось прочитать строки Excel.")
	}
	header, err := readHeaderFromRows(rows)
	if err != nil {
		return nil, err
	}
	cols, err := findColumns(header)
	if err != nil {
		return nil, err
	}

	rowCount := max(len(rows)-1, 0)
	if rowCount == 0 {
		return nil, fmt.Errorf("В файле нет строк для %s.", cfg.processName)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 12, fmt.Sprintf("Найдено строк в Excel: %d. Группирую материалы по ИИН, количеству и сумме задолженности", rowCount))
	groupsByKey, groups, loads, loginIINs, fixedCount, fixedIINCount := collectGroupsFromRows(workbook, sheet, rows, cols, cfg.fixedStatuses, cfg.sourceColumn)
	if len(groupsByKey) == 0 {
		return nil, fmt.Errorf("Нет строк для %s после фиксации материалов по статусам.", cfg.processName)
	}

	loginKeys := readLoginKeysFromRows(rows, cols, cfg.sourceColumn)
	if len(loginKeys) == 0 {
		return nil, errors.New("Не найдены логины для распределения.")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 32, fmt.Sprintf("Логинов: %d, ИИН в ротации: %d, зафиксировано строк: %d, зафиксировано ИИН: %d", len(loginKeys), len(groupsByKey), fixedCount, fixedIINCount))
	ensureLoadsForAllLogins(loads, loginIINs, loginKeys)

	app.sendProgress(jobID, 48, "Считаю нагрузку по логинам и подбираю распределение по РП")
	var assignments map[string]loginKey
	progress := func(percent int, message string) {
		app.sendProgress(jobID, percent, message)
	}
	if cfg.strategy == "partial" {
		assignments, err = partiallyBalanceGroups(ctx, groups, loginKeys, loads, loginIINs, progress)
	} else {
		assignments, err = balanceGroups(ctx, groups, loginKeys, loads, loginIINs, progress)
	}
	if err != nil {
		return nil, err
	}

	rotationRowCount := 0
	for _, group := range groupsByKey {
		rotationRowCount += len(group.rows)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 70, fmt.Sprintf("Распределено ИИН: %d. Заполняю колонку \"Закрепить\" для %d строк", len(assignments), rotationRowCount))
	for key, group := range groupsByKey {
		login := assignments[key]
		for _, rowNumber := range group.rows {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			_ = setCell(workbook, sheet, rowNumber, cols.attach, login.login)
		}
	}

	_ = styleAttachColumn(workbook, sheet, cols.attach)

	app.sendProgress(jobID, 84, "Формирую лист с итогами")
	_ = replaceSummarySheet(workbook, loads, fixedCount, fixedIINCount, cfg.summaryTitle)

	var output bytes.Buffer
	if err := workbook.Write(&output); err != nil {
		return nil, errors.New("Не удалось сохранить Excel файл.")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	app.sendProgress(jobID, 100, fmt.Sprintf("Процесс %s завершен", cfg.processName))
	return output.Bytes(), nil
}

func readHeader(workbook *excelize.File, sheet string) (map[string]int, error) {
	rows, err := workbook.GetRows(sheet)
	if err != nil || len(rows) == 0 {
		return nil, errors.New("Не удалось прочитать заголовок Excel.")
	}
	return readHeaderFromRows(rows)
}

func readHeaderFromRows(rows [][]string) (map[string]int, error) {
	if len(rows) == 0 {
		return nil, errors.New("Не удалось прочитать заголовок Excel.")
	}
	header := make(map[string]int)
	for i, value := range rows[0] {
		if strings.TrimSpace(value) == "" {
			continue
		}
		header[normalizeHeader(value)] = i + 1
	}
	return header, nil
}

func findColumns(header map[string]int) (columns, error) {
	cols := columns{
		rp:     header["рп"],
		iin:    header["иин"],
		detach: header["открепить"],
		attach: header["закрепить"],
		status: header["статус"],
	}
	for name, col := range header {
		if strings.HasPrefix(name, "общая задолженность") {
			cols.amount = col
			break
		}
	}

	var missing []string
	checks := []struct {
		col   int
		title string
	}{
		{cols.rp, "РП"},
		{cols.iin, "ИИН"},
		{cols.amount, "Общая задолженность"},
		{cols.detach, "Открепить"},
		{cols.attach, "Закрепить"},
		{cols.status, "Статус"},
	}
	for _, item := range checks {
		if item.col == 0 {
			missing = append(missing, item.title)
		}
	}
	if len(missing) > 0 {
		return cols, errors.New("Не найдены колонки: " + strings.Join(missing, ", "))
	}
	return cols, nil
}

func collectGroups(workbook *excelize.File, sheet string, maxRow int, cols columns, fixedStatuses map[string]bool, sourceColumn string) (map[string]*iinGroup, []*iinGroup, map[loginKey]*load, map[loginKey]map[string]bool, int, int) {
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		rows = nil
	}
	if maxRow > 0 && maxRow < len(rows) {
		rows = rows[:maxRow]
	}
	return collectGroupsFromRows(workbook, sheet, rows, cols, fixedStatuses, sourceColumn)
}

func collectGroupsFromRows(workbook *excelize.File, sheet string, rows [][]string, cols columns, fixedStatuses map[string]bool, sourceColumn string) (map[string]*iinGroup, []*iinGroup, map[loginKey]*load, map[loginKey]map[string]bool, int, int) {
	groups := make(map[string]*iinGroup)
	groupOrder := make([]*iinGroup, 0)
	loads := make(map[loginKey]*load)
	loginIINs := make(map[loginKey]map[string]bool)
	pinnedIINs := make(map[string]map[string]bool)
	fixedIINs := make(map[string]bool)
	fixedCount := 0

	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		rowNumber := rowIndex + 1
		row := rows[rowIndex]
		rp := normalizeRP(getRowCell(row, cols.rp))
		iin := normalizeIIN(getRowCell(row, cols.iin))
		currentLogin := readSourceLoginFromRow(row, cols, sourceColumn)
		if rp == "" || iin == "" || currentLogin == "" {
			continue
		}

		groupKey := makeGroupKey(rp, iin)
		login := loginKey{rp: rp, login: currentLogin}
		amount := toDecimal(getRowCell(row, cols.amount))
		status := normalizeStatus(getRowCell(row, cols.status))
		if fixedStatuses[status] {
			addLoad(loads, loginIINs, login, iin, amount, 1)
			_ = setCell(workbook, sheet, rowNumber, cols.attach, currentLogin)
			if pinnedIINs[groupKey] == nil {
				pinnedIINs[groupKey] = make(map[string]bool)
			}
			pinnedIINs[groupKey][currentLogin] = true
			fixedIINs[groupKey] = true
			fixedCount++
			continue
		}

		if groups[groupKey] == nil {
			groups[groupKey] = &iinGroup{rp: rp, iin: iin, currentLogin: currentLogin}
			groupOrder = append(groupOrder, groups[groupKey])
		}
		groups[groupKey].rows = append(groups[groupKey].rows, rowNumber)
		groups[groupKey].amount = pyAdd(groups[groupKey].amount, amount)
	}

	for groupKey, logins := range pinnedIINs {
		if group, ok := groups[groupKey]; ok && len(logins) == 1 {
			for login := range logins {
				group.pinnedLogin = login
			}
		}
	}
	return groups, groupOrder, loads, loginIINs, fixedCount, len(fixedIINs)
}

func readLoginKeys(workbook *excelize.File, sheet string, maxRow int, cols columns, sourceColumn string) []loginKey {
	rows, err := workbook.GetRows(sheet)
	if err != nil {
		return nil
	}
	if maxRow > 0 && maxRow < len(rows) {
		rows = rows[:maxRow]
	}
	return readLoginKeysFromRows(rows, cols, sourceColumn)
}

func readLoginKeysFromRows(rows [][]string, cols columns, sourceColumn string) []loginKey {
	var keys []loginKey
	seen := make(map[loginKey]bool)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		row := rows[rowIndex]
		rp := normalizeRP(getRowCell(row, cols.rp))
		login := readSourceLoginFromRow(row, cols, sourceColumn)
		key := loginKey{rp: rp, login: login}
		if rp != "" && login != "" && !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].rp == keys[j].rp {
			return keys[i].login < keys[j].login
		}
		return keys[i].rp < keys[j].rp
	})
	return keys
}

func readSourceLogin(workbook *excelize.File, sheet string, rowNumber int, cols columns, sourceColumn string) string {
	if sourceColumn == "attach" {
		attachedLogin := normalizeLogin(getCell(workbook, sheet, rowNumber, cols.attach))
		if attachedLogin != "" {
			return attachedLogin
		}
	}
	return normalizeLogin(getCell(workbook, sheet, rowNumber, cols.detach))
}

func readSourceLoginFromRow(row []string, cols columns, sourceColumn string) string {
	if sourceColumn == "attach" {
		attachedLogin := normalizeLogin(getRowCell(row, cols.attach))
		if attachedLogin != "" {
			return attachedLogin
		}
	}
	return normalizeLogin(getRowCell(row, cols.detach))
}

func ensureLoadsForAllLogins(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeys []loginKey) {
	for _, key := range loginKeys {
		if loads[key] == nil {
			loads[key] = &load{}
		}
		if loginIINs[key] == nil {
			loginIINs[key] = make(map[string]bool)
		}
	}
}

func addLoad(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, key loginKey, iin string, amount decimal.Decimal, materialCount int) {
	if loads[key] == nil {
		loads[key] = &load{}
	}
	if loginIINs[key] == nil {
		loginIINs[key] = make(map[string]bool)
	}
	loads[key].count += materialCount
	loads[key].amount = pyAdd(loads[key].amount, amount)
	if !loginIINs[key][iin] {
		loginIINs[key][iin] = true
		loads[key].iinCount++
	}
}

func removeLoad(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, key loginKey, iin string, amount decimal.Decimal, materialCount int) {
	loads[key].count -= materialCount
	loads[key].amount = pySub(loads[key].amount, amount)
	if loginIINs[key][iin] {
		delete(loginIINs[key], iin)
		loads[key].iinCount--
	}
}

func balanceGroups(ctx context.Context, groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, progress progressFunc) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	loginKeysByRP := groupLoginKeysByRP(loginKeys)
	targetsByRP := make(map[string][3]decimal.Decimal)
	for rp, rpLoginKeys := range loginKeysByRP {
		targetsByRP[rp] = targetsForRP(groupsForRP(groups, rp), rpLoginKeys, loads, loginIINs)
	}
	reportProgress(progress, 50, "Целевая нагрузка по РП рассчитана")

	sort.SliceStable(groups, func(i, j int) bool {
		return groupWeight(groups[i], targetsByRP[groups[i].rp]).GreaterThan(groupWeight(groups[j], targetsByRP[groups[j].rp]))
	})
	reportProgress(progress, 52, fmt.Sprintf("Группы отсортированы по весу: %d ИИН", len(groups)))

	counter := newProgressCounter(progress, 52, 62, len(groups), "Первичное распределение ИИН")
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupKey := makeGroupKey(group.rp, group.iin)
		rpLoginKeys := loginKeysByRP[group.rp]
		if len(rpLoginKeys) == 0 {
			return nil, fmt.Errorf("Для РП %q нет логинов для распределения.", group.rp)
		}
		targets := targetsByRP[group.rp]
		var selected loginKey
		if group.pinnedLogin != "" {
			selected = loginKey{rp: group.rp, login: group.pinnedLogin}
		} else {
			for index, candidate := range rpLoginKeys {
				score := scoreAfterAdd(loads, loginIINs, rpLoginKeys, candidate, group, targets, rotationScoreWeights)
				if index == 0 || score.LessThan(scoreAfterAdd(loads, loginIINs, rpLoginKeys, selected, group, targets, rotationScoreWeights)) {
					selected = candidate
				}
			}
		}
		assignments[groupKey] = selected
		addLoad(loads, loginIINs, selected, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}

	if err := improveAssignmentsExact(ctx, groups, assignments, loads, loginIINs, loginKeysByRP, targetsByRP, rotationScoreWeights, progress); err != nil {
		return nil, err
	}
	return assignments, nil
}

func balanceGroupsForRP(groups []*iinGroup, rpLoginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, targets [3]decimal.Decimal, counter *progressCounter) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	for _, group := range groups {
		groupKey := makeGroupKey(group.rp, group.iin)
		if len(rpLoginKeys) == 0 {
			return nil, fmt.Errorf("Для РП %q нет логинов для распределения.", group.rp)
		}
		var selected loginKey
		if group.pinnedLogin != "" {
			selected = loginKey{rp: group.rp, login: group.pinnedLogin}
		} else {
			baseScores := loginScoresForLogins(loads, rpLoginKeys, targets, rotationScoreWeights)
			selected, _ = bestCandidateAfterAdd(loads, loginIINs, rpLoginKeys, baseScores, group, targets, rotationScoreWeights)
		}
		assignments[groupKey] = selected
		addLoad(loads, loginIINs, selected, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}
	return assignments, nil
}

func partiallyBalanceGroups(ctx context.Context, groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, progress progressFunc) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	loginKeysByRP := groupLoginKeysByRP(loginKeys)
	targetsByRP := make(map[string][3]decimal.Decimal)
	for rp, rpLoginKeys := range loginKeysByRP {
		targetsByRP[rp] = targetsForRP(groupsForRP(groups, rp), rpLoginKeys, loads, loginIINs)
	}
	reportProgress(progress, 50, "Целевая нагрузка по РП рассчитана")

	counter := newProgressCounter(progress, 50, 58, len(groups), "Читаю текущее закрепление ИИН")
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupKey := makeGroupKey(group.rp, group.iin)
		login := loginKey{rp: group.rp, login: firstNonEmpty(group.pinnedLogin, group.currentLogin)}
		if !containsLoginKey(loginKeysByRP[group.rp], login) {
			return nil, fmt.Errorf("Для ИИН %s не найден текущий логин в РП %q.", group.iin, group.rp)
		}
		assignments[groupKey] = login
		addLoad(loads, loginIINs, login, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}

	if err := improvePartialAssignmentsExact(ctx, groups, assignments, loads, loginIINs, loginKeysByRP, targetsByRP, alignmentScoreWeights, alignmentMoveLimits(groups), progress); err != nil {
		return nil, err
	}
	return assignments, nil
}

func partialAssignmentsForRP(groups []*iinGroup, rpLoginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, counter *progressCounter) (map[string]loginKey, error) {
	assignments := make(map[string]loginKey)
	for _, group := range groups {
		groupKey := makeGroupKey(group.rp, group.iin)
		login := loginKey{rp: group.rp, login: firstNonEmpty(group.pinnedLogin, group.currentLogin)}
		if !containsLoginKey(rpLoginKeys, login) {
			return nil, fmt.Errorf("Для ИИН %s не найден текущий логин в РП %q.", group.iin, group.rp)
		}
		assignments[groupKey] = login
		addLoad(loads, loginIINs, login, group.iin, group.amount, len(group.rows))
		counter.add(1)
	}
	return assignments, nil
}

func groupLoginKeysByRP(loginKeys []loginKey) map[string][]loginKey {
	out := make(map[string][]loginKey)
	for _, key := range loginKeys {
		out[key.rp] = append(out[key.rp], key)
	}
	return out
}

func groupGroupsByRP(groups []*iinGroup) map[string][]*iinGroup {
	out := make(map[string][]*iinGroup)
	for _, group := range groups {
		out[group.rp] = append(out[group.rp], group)
	}
	return out
}

func clonePortfolioForLogins(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeys []loginKey) (map[loginKey]*load, map[loginKey]map[string]bool) {
	clonedLoads := make(map[loginKey]*load, len(loginKeys))
	clonedLoginIINs := make(map[loginKey]map[string]bool, len(loginKeys))
	for _, key := range loginKeys {
		sourceLoad := loads[key]
		if sourceLoad == nil {
			sourceLoad = &load{}
		}
		clonedLoads[key] = &load{
			count:    sourceLoad.count,
			amount:   sourceLoad.amount,
			iinCount: sourceLoad.iinCount,
		}
		clonedLoginIINs[key] = cloneStringSet(loginIINs[key])
	}
	return clonedLoads, clonedLoginIINs
}

func cloneStringSet(source map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(source))
	for value := range source {
		cloned[value] = true
	}
	return cloned
}

func mergeAssignments(target map[string]loginKey, source map[string]loginKey) {
	for key, value := range source {
		target[key] = value
	}
}

func mergePortfolio(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, sourceLoads map[loginKey]*load, sourceLoginIINs map[loginKey]map[string]bool) {
	for key, sourceLoad := range sourceLoads {
		loads[key] = &load{
			count:    sourceLoad.count,
			amount:   sourceLoad.amount,
			iinCount: sourceLoad.iinCount,
		}
		loginIINs[key] = cloneStringSet(sourceLoginIINs[key])
	}
}

func filterAssignmentsForGroups(assignments map[string]loginKey, groups []*iinGroup) map[string]loginKey {
	filtered := make(map[string]loginKey, len(groups))
	for _, group := range groups {
		groupKey := makeGroupKey(group.rp, group.iin)
		filtered[groupKey] = assignments[groupKey]
	}
	return filtered
}

func targetsForRP(groups []*iinGroup, loginKeys []loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool) [3]decimal.Decimal {
	loginCount := decimal.NewFromInt(int64(len(loginKeys)))
	totalCount := 0
	totalIIN := 0
	totalAmount := decimal.Zero
	for _, key := range loginKeys {
		totalCount += loads[key].count
		totalIIN += loads[key].iinCount
		totalAmount = pyAdd(totalAmount, loads[key].amount)
	}
	for _, group := range groups {
		totalCount += len(group.rows)
		totalAmount = pyAdd(totalAmount, group.amount)
		if group.pinnedLogin == "" || !loginIINs[loginKey{rp: group.rp, login: group.pinnedLogin}][group.iin] {
			totalIIN++
		}
	}
	if loginCount.IsZero() {
		return [3]decimal.Decimal{decimal.Zero, decimal.NewFromInt(1), decimal.Zero}
	}
	targetCount := pyDiv(decimal.NewFromInt(int64(totalCount)), loginCount)
	targetIIN := pyDiv(decimal.NewFromInt(int64(totalIIN)), loginCount)
	targetAmount := pyDiv(totalAmount, loginCount)
	if targetAmount.IsZero() {
		targetAmount = decimal.NewFromInt(1)
	}
	return [3]decimal.Decimal{targetIIN, targetAmount, targetCount}
}

func groupWeight(group *iinGroup, targets [3]decimal.Decimal) decimal.Decimal {
	targetAmount := targets[1]
	targetCount := targets[2]
	amountWeight := decimal.Zero
	countWeight := decimal.Zero
	if !targetAmount.IsZero() {
		amountWeight = pyDiv(group.amount, targetAmount)
	}
	if !targetCount.IsZero() {
		countWeight = pyDiv(decimal.NewFromInt(int64(len(group.rows))), targetCount)
	}
	return pyAdd(pyAdd(amountWeight, countWeight), decimal.NewFromInt(1))
}

func scoreAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, logins []loginKey, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	score := decimal.Zero
	for _, item := range logins {
		itemLoad := loads[item]
		amount := itemLoad.amount
		count := itemLoad.count
		iinCount := itemLoad.iinCount
		if item == login {
			amount = pyAdd(amount, group.amount)
			count += len(group.rows)
			if !loginIINs[item][group.iin] {
				iinCount++
			}
		}
		score = pyAdd(score, loadScore(iinCount, amount, count, targets, weights))
	}
	return score
}

func scoreAfterAddFromScores(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, baseScore decimal.Decimal, baseScores map[loginKey]decimal.Decimal, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pyAdd(item.amount, group.amount)
	count := item.count + len(group.rows)
	iinCount := item.iinCount
	if !loginIINs[login][group.iin] {
		iinCount++
	}
	return pyAdd(pySub(baseScore, baseScores[login]), loadScore(iinCount, amount, count, targets, weights))
}

func bestCandidateAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidates []loginKey, baseScores map[loginKey]decimal.Decimal, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) (loginKey, decimal.Decimal) {
	return bestCandidateAfterAddWithBaseline(loads, loginIINs, candidates, baseScores, group, targets, weights, candidates[0], decimal.Zero, false)
}

func bestCandidateAfterAddWithBaseline(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidates []loginKey, baseScores map[loginKey]decimal.Decimal, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights, defaultLogin loginKey, defaultScore decimal.Decimal, hasDefault bool) (loginKey, decimal.Decimal) {
	baseScore := scoreFromScores(candidates, baseScores)
	if len(candidates) < 12 {
		bestLogin := defaultLogin
		bestScore := defaultScore
		hasBest := hasDefault
		for _, candidate := range candidates {
			score := scoreAfterAddFromScores(loads, loginIINs, baseScore, baseScores, candidate, group, targets, weights)
			if !hasBest || score.LessThan(bestScore) {
				bestScore = score
				bestLogin = candidate
				hasBest = true
			}
		}
		return bestLogin, bestScore
	}

	scores := make([]decimal.Decimal, len(candidates))
	workerCount := min(runtime.GOMAXPROCS(0), len(candidates))
	jobs := make(chan int, len(candidates))
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				candidate := candidates[index]
				scores[index] = scoreAfterAddFromScores(loads, loginIINs, baseScore, baseScores, candidate, group, targets, weights)
			}
		}()
	}
	for index := range candidates {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	bestLogin := defaultLogin
	bestScore := defaultScore
	hasBest := hasDefault
	for index, candidateScore := range scores {
		if !hasBest || candidateScore.LessThan(bestScore) {
			bestScore = candidateScore
			bestLogin = candidates[index]
			hasBest = true
		}
	}
	return bestLogin, bestScore
}

func loginScoresForLogins(loads map[loginKey]*load, logins []loginKey, targets [3]decimal.Decimal, weights scoreWeights) map[loginKey]decimal.Decimal {
	scores := make(map[loginKey]decimal.Decimal, len(logins))
	for _, login := range logins {
		scores[login] = loginScore(loads, login, targets, weights)
	}
	return scores
}

func cloneScoreMap(source map[loginKey]decimal.Decimal) map[loginKey]decimal.Decimal {
	cloned := make(map[loginKey]decimal.Decimal, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func scoreFromScores(logins []loginKey, scores map[loginKey]decimal.Decimal) decimal.Decimal {
	score := decimal.Zero
	for _, login := range logins {
		score = pyAdd(score, scores[login])
	}
	return score
}

func loginScoreAfterAdd(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pyAdd(item.amount, group.amount)
	count := item.count + len(group.rows)
	iinCount := item.iinCount
	if !loginIINs[login][group.iin] {
		iinCount++
	}
	return loadScore(iinCount, amount, count, targets, weights)
}

func loadScore(iinCount int, amount decimal.Decimal, materialCount int, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	targetIIN, targetAmount, targetCount := targets[0], targets[1], targets[2]
	amountDelta := decimal.Zero
	countDelta := decimal.Zero
	iinDelta := decimal.Zero
	if !targetAmount.IsZero() {
		amountDelta = pyDiv(pySub(amount, targetAmount), targetAmount)
	}
	if !targetCount.IsZero() {
		countDelta = pyDiv(pySub(decimal.NewFromInt(int64(materialCount)), targetCount), targetCount)
	}
	if !targetIIN.IsZero() {
		iinDelta = pyDiv(pySub(decimal.NewFromInt(int64(iinCount)), targetIIN), targetIIN)
	}
	amountScore := pyMul(pyMul(amountDelta, amountDelta), weights.amount)
	countScore := pyMul(pyMul(countDelta, countDelta), weights.materialCount)
	iinScore := pyMul(pyMul(iinDelta, iinDelta), weights.iinCount)
	return pyAdd(pyAdd(amountScore, countScore), iinScore)
}

func loginScore(loads map[loginKey]*load, login loginKey, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	return loadScore(item.iinCount, item.amount, item.count, targets, weights)
}

func portfolioScore(loads map[loginKey]*load, logins []loginKey, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	score := decimal.Zero
	for _, login := range logins {
		score = pyAdd(score, loginScore(loads, login, targets, weights))
	}
	return score
}

func improveAssignments(groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, progress progressFunc, counter *progressCounter) {
	var movableGroups []*iinGroup
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableGroups = append(movableGroups, group)
		}
	}
	reportProgress(progress, 63, fmt.Sprintf("Уточняю распределение: можно переместить %d ИИН", len(movableGroups)))

	for i := 0; i < 4; i++ {
		changed := false
		lastPercent := 63 + i
		for index, group := range movableGroups {
			groupKey := makeGroupKey(group.rp, group.iin)
			rpLoginKeys := loginKeysByRP[group.rp]
			targets := targetsByRP[group.rp]
			currentScores := loginScoresForLogins(loads, rpLoginKeys, targets, weights)
			currentScore := scoreFromScores(rpLoginKeys, currentScores)
			currentLogin := assignments[groupKey]
			bestLogin := currentLogin
			bestScore := currentScore

			removeLoad(loads, loginIINs, currentLogin, group.iin, group.amount, len(group.rows))
			removedScores := cloneScoreMap(currentScores)
			removedScores[currentLogin] = loginScore(loads, currentLogin, targets, weights)
			bestLogin, bestScore = bestCandidateAfterAddWithBaseline(loads, loginIINs, rpLoginKeys, removedScores, group, targets, weights, bestLogin, bestScore, true)

			addLoad(loads, loginIINs, bestLogin, group.iin, group.amount, len(group.rows))
			if bestLogin != currentLogin {
				assignments[groupKey] = bestLogin
				changed = true
			}
			counter.add(1)
			lastPercent = reportRangeProgress(
				progress,
				63+i,
				64+i,
				index+1,
				len(movableGroups),
				lastPercent,
				fmt.Sprintf("Уточнение распределения, проход %d из 4", i+1),
			)
		}
		if !changed {
			reportProgress(progress, 68, "Уточнение завершено: улучшений больше нет")
			break
		}
	}
}

func improveAssignmentsExact(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, progress progressFunc) error {
	var movableGroups []*iinGroup
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableGroups = append(movableGroups, group)
		}
	}
	reportProgress(progress, 63, fmt.Sprintf("Уточняю распределение: можно переместить %d ИИН", len(movableGroups)))

	counter := newProgressCounter(progress, 63, 68, max(len(movableGroups)*4, 1), "Уточнение распределения")
	for i := 0; i < 4; i++ {
		changed := false
		for _, group := range movableGroups {
			if err := ctx.Err(); err != nil {
				return err
			}
			groupKey := makeGroupKey(group.rp, group.iin)
			rpLoginKeys := loginKeysByRP[group.rp]
			targets := targetsByRP[group.rp]
			currentScore := portfolioScore(loads, rpLoginKeys, targets, weights)
			currentLogin := assignments[groupKey]
			bestLogin := currentLogin
			bestScore := currentScore

			removeLoad(loads, loginIINs, currentLogin, group.iin, group.amount, len(group.rows))
			for _, candidate := range rpLoginKeys {
				addLoad(loads, loginIINs, candidate, group.iin, group.amount, len(group.rows))
				candidateScore := portfolioScore(loads, rpLoginKeys, targets, weights)
				removeLoad(loads, loginIINs, candidate, group.iin, group.amount, len(group.rows))
				if candidateScore.LessThan(bestScore) {
					bestScore = candidateScore
					bestLogin = candidate
				}
			}

			addLoad(loads, loginIINs, bestLogin, group.iin, group.amount, len(group.rows))
			if bestLogin != currentLogin {
				assignments[groupKey] = bestLogin
				changed = true
			}
			counter.add(1)
		}
		if !changed {
			reportProgress(progress, 68, "Уточнение завершено: улучшений больше нет")
			break
		}
	}
	return nil
}

func improveAssignmentsParallel(groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, progress progressFunc) {
	var movableCount int
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableCount++
		}
	}
	reportProgress(progress, 63, fmt.Sprintf("Уточняю распределение параллельно по РП: можно переместить %d ИИН", movableCount))

	groupsByRP := groupGroupsByRP(groups)
	results := make(chan rpBalanceResult, len(groupsByRP))
	counter := newProgressCounter(progress, 63, 68, movableCount*4, "Уточнение распределения")
	for rp, rpGroups := range groupsByRP {
		rp := rp
		rpGroups := rpGroups
		rpLoginKeys := loginKeysByRP[rp]
		localLoads, localLoginIINs := clonePortfolioForLogins(loads, loginIINs, rpLoginKeys)
		localAssignments := filterAssignmentsForGroups(assignments, rpGroups)
		go func() {
			improveAssignments(rpGroups, localAssignments, localLoads, localLoginIINs, map[string][]loginKey{rp: rpLoginKeys}, map[string][3]decimal.Decimal{rp: targetsByRP[rp]}, weights, nil, counter)
			results <- rpBalanceResult{
				rp:          rp,
				assignments: localAssignments,
				loads:       localLoads,
				loginIINs:   localLoginIINs,
			}
		}()
	}

	completedGroups := 0
	lastPercent := 63
	for range groupsByRP {
		result := <-results
		mergeAssignments(assignments, result.assignments)
		mergePortfolio(loads, loginIINs, result.loads, result.loginIINs)
		completedGroups += len(groupsByRP[result.rp])
		lastPercent = reportRangeProgress(
			progress,
			63,
			68,
			completedGroups,
			len(groups),
			lastPercent,
			"Уточнение распределения по РП",
		)
	}
	reportProgress(progress, 68, "Уточнение завершено")
}

func improvePartialAssignments(groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, moveLimitsByRP map[string]int, progress progressFunc, counter *progressCounter) {
	movableByRP := make(map[string][]*iinGroup)
	for _, group := range groups {
		if group.pinnedLogin == "" {
			movableByRP[group.rp] = append(movableByRP[group.rp], group)
		}
	}

	movedGroups := make(map[string]bool)
	totalLimit := 0
	for _, limit := range moveLimitsByRP {
		totalLimit += limit
	}
	checkedMoves := 0
	lastPercent := 58
	reportProgress(progress, 59, fmt.Sprintf("Ищу полезные перемещения ИИН, лимит перемещений: %d", totalLimit))
	for rp, groupsInRP := range movableByRP {
		rpLoginKeys := loginKeysByRP[rp]
		targets := targetsByRP[rp]
		moveLimit := moveLimitsByRP[rp]

		for i := 0; i < moveLimit; i++ {
			checkedMoves++
			baseScores := loginScoresForLogins(loads, rpLoginKeys, targets, weights)
			currentScore := scoreFromScores(rpLoginKeys, baseScores)
			bestGroup, bestLogin, ok := bestPartialMoveCandidate(groupsInRP, assignments, loads, loginIINs, rpLoginKeys, baseScores, targets, weights, movedGroups, currentScore)
			if counter != nil {
				counter.add(1)
			}
			if !ok {
				break
			}
			groupKey := makeGroupKey(bestGroup.rp, bestGroup.iin)
			currentLogin := assignments[groupKey]
			removeLoad(loads, loginIINs, currentLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			addLoad(loads, loginIINs, bestLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			assignments[groupKey] = bestLogin
			movedGroups[groupKey] = true
			if counter == nil {
				lastPercent = reportRangeProgress(
					progress,
					59,
					68,
					checkedMoves,
					max(totalLimit, 1),
					lastPercent,
					fmt.Sprintf("Выравнивание РП %s: перемещено ИИН %d", rp, len(movedGroups)),
				)
			}
		}
	}
	reportProgress(progress, 68, fmt.Sprintf("Выравнивание подобрано, перемещено ИИН: %d", len(movedGroups)))
}

func improvePartialAssignmentsExact(ctx context.Context, groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, moveLimitsByRP map[string]int, progress progressFunc) error {
	movableByRP := make(map[string][]*iinGroup)
	var rpOrder []string
	seenRP := make(map[string]bool)
	for _, group := range groups {
		if group.pinnedLogin != "" {
			continue
		}
		if !seenRP[group.rp] {
			seenRP[group.rp] = true
			rpOrder = append(rpOrder, group.rp)
		}
		movableByRP[group.rp] = append(movableByRP[group.rp], group)
	}

	totalLimit := 0
	for _, limit := range moveLimitsByRP {
		totalLimit += limit
	}
	reportProgress(progress, 59, fmt.Sprintf("Ищу полезные перемещения ИИН, лимит перемещений: %d", totalLimit))

	movedGroups := make(map[string]bool)
	counter := newProgressCounter(progress, 59, 68, max(totalLimit, 1), "Выравнивание ИИН")
	for _, rp := range rpOrder {
		groupsInRP := movableByRP[rp]
		rpLoginKeys := loginKeysByRP[rp]
		targets := targetsByRP[rp]
		moveLimit := moveLimitsByRP[rp]

		for i := 0; i < moveLimit; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			var bestGroup *iinGroup
			var bestLogin loginKey
			currentScore := portfolioScore(loads, rpLoginKeys, targets, weights)
			bestScore := currentScore

			for _, group := range groupsInRP {
				groupKey := makeGroupKey(group.rp, group.iin)
				if movedGroups[groupKey] {
					continue
				}

				currentLogin := assignments[groupKey]
				currentLoginScore := loginScore(loads, currentLogin, targets, weights)
				removeLoad(loads, loginIINs, currentLogin, group.iin, group.amount, len(group.rows))
				currentRemovedScore := loginScore(loads, currentLogin, targets, weights)
				for _, candidate := range rpLoginKeys {
					if candidate == currentLogin {
						continue
					}
					candidateScoreBefore := loginScore(loads, candidate, targets, weights)
					addLoad(loads, loginIINs, candidate, group.iin, group.amount, len(group.rows))
					candidateScore := pyAdd(
						pyAdd(
							pySub(
								pySub(currentScore, currentLoginScore),
								candidateScoreBefore,
							),
							currentRemovedScore,
						),
						loginScore(loads, candidate, targets, weights),
					)
					removeLoad(loads, loginIINs, candidate, group.iin, group.amount, len(group.rows))
					if candidateScore.LessThan(bestScore) {
						bestScore = candidateScore
						bestGroup = group
						bestLogin = candidate
					}
				}
				addLoad(loads, loginIINs, currentLogin, group.iin, group.amount, len(group.rows))
			}
			counter.add(1)

			if bestGroup == nil {
				break
			}

			groupKey := makeGroupKey(bestGroup.rp, bestGroup.iin)
			currentLogin := assignments[groupKey]
			removeLoad(loads, loginIINs, currentLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			addLoad(loads, loginIINs, bestLogin, bestGroup.iin, bestGroup.amount, len(bestGroup.rows))
			assignments[groupKey] = bestLogin
			movedGroups[groupKey] = true
		}
	}
	reportProgress(progress, 68, fmt.Sprintf("Выравнивание подобрано, перемещено ИИН: %d", len(movedGroups)))
	return nil
}

func improvePartialAssignmentsParallel(groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, loginKeysByRP map[string][]loginKey, targetsByRP map[string][3]decimal.Decimal, weights scoreWeights, moveLimitsByRP map[string]int, progress progressFunc) {
	totalLimit := 0
	for _, limit := range moveLimitsByRP {
		totalLimit += limit
	}
	reportProgress(progress, 59, fmt.Sprintf("Ищу полезные перемещения параллельно по РП, лимит перемещений: %d", totalLimit))

	groupsByRP := groupGroupsByRP(groups)
	results := make(chan rpBalanceResult, len(groupsByRP))
	counter := newProgressCounter(progress, 59, 68, max(totalLimit, 1), "Выравнивание ИИН")
	for rp, rpGroups := range groupsByRP {
		rp := rp
		rpGroups := rpGroups
		rpLoginKeys := loginKeysByRP[rp]
		localLoads, localLoginIINs := clonePortfolioForLogins(loads, loginIINs, rpLoginKeys)
		localAssignments := filterAssignmentsForGroups(assignments, rpGroups)
		go func() {
			improvePartialAssignments(
				rpGroups,
				localAssignments,
				localLoads,
				localLoginIINs,
				map[string][]loginKey{rp: rpLoginKeys},
				map[string][3]decimal.Decimal{rp: targetsByRP[rp]},
				weights,
				map[string]int{rp: moveLimitsByRP[rp]},
				nil,
				counter,
			)
			results <- rpBalanceResult{
				rp:          rp,
				assignments: localAssignments,
				loads:       localLoads,
				loginIINs:   localLoginIINs,
			}
		}()
	}

	completedGroups := 0
	lastPercent := 59
	for range groupsByRP {
		result := <-results
		mergeAssignments(assignments, result.assignments)
		mergePortfolio(loads, loginIINs, result.loads, result.loginIINs)
		completedGroups += len(groupsByRP[result.rp])
		lastPercent = reportRangeProgress(
			progress,
			59,
			68,
			completedGroups,
			len(groups),
			lastPercent,
			"Выравнивание по РП",
		)
	}
	reportProgress(progress, 68, "Выравнивание подобрано")
}

func bestPartialMoveCandidate(groups []*iinGroup, assignments map[string]loginKey, loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, rpLoginKeys []loginKey, baseScores map[loginKey]decimal.Decimal, targets [3]decimal.Decimal, weights scoreWeights, movedGroups map[string]bool, currentScore decimal.Decimal) (*iinGroup, loginKey, bool) {
	bestScore := currentScore
	var bestGroup *iinGroup
	var bestLogin loginKey

	for _, group := range groups {
		groupKey := makeGroupKey(group.rp, group.iin)
		if movedGroups[groupKey] {
			continue
		}
		currentLogin := assignments[groupKey]
		currentLoginScore := baseScores[currentLogin]
		currentRemovedScore := loginScoreAfterRemove(loads, loginIINs, currentLogin, group, targets, weights)
		for _, candidate := range rpLoginKeys {
			if candidate == currentLogin {
				continue
			}
			score := partialMoveScoreFromBase(loads, loginIINs, candidate, group, targets, weights, currentScore, currentLoginScore, currentRemovedScore, baseScores[candidate])
			if score.LessThan(bestScore) {
				bestScore = score
				bestGroup = group
				bestLogin = candidate
			}
		}
	}
	return bestGroup, bestLogin, bestGroup != nil
}

func partialMoveScoreFromBase(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, candidate loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights, currentScore decimal.Decimal, currentLoginScore decimal.Decimal, currentRemovedScore decimal.Decimal, candidateScoreBefore decimal.Decimal) decimal.Decimal {
	candidateScoreAfter := loginScoreAfterAdd(loads, loginIINs, candidate, group, targets, weights)
	return pyAdd(
		pyAdd(
			pySub(
				pySub(currentScore, currentLoginScore),
				candidateScoreBefore,
			),
			currentRemovedScore,
		),
		candidateScoreAfter,
	)
}

func loginScoreAfterRemove(loads map[loginKey]*load, loginIINs map[loginKey]map[string]bool, login loginKey, group *iinGroup, targets [3]decimal.Decimal, weights scoreWeights) decimal.Decimal {
	item := loads[login]
	amount := pySub(item.amount, group.amount)
	count := item.count - len(group.rows)
	iinCount := item.iinCount
	if loginIINs[login][group.iin] {
		iinCount--
	}
	return loadScore(iinCount, amount, count, targets, weights)
}

func alignmentMoveLimits(groups []*iinGroup) map[string]int {
	counts := make(map[string]int)
	for _, group := range groups {
		if group.pinnedLogin == "" {
			counts[group.rp]++
		}
	}
	limits := make(map[string]int)
	for rp, count := range counts {
		limits[rp] = max(1, int(math.Ceil(float64(count)*0.10)))
	}
	return limits
}

func replaceSummarySheet(workbook *excelize.File, loads map[loginKey]*load, fixedCount int, fixedIINCount int, title string) error {
	if index, err := workbook.GetSheetIndex(title); err == nil && index != -1 {
		_ = workbook.DeleteSheet(title)
	}
	_, err := workbook.NewSheet(title)
	if err != nil {
		return err
	}
	headers := []any{"РП", "Логин", "Количество материалов", "Сумма задолженности", "Количество ИИН"}
	for i, value := range headers {
		if err := setCell(workbook, title, 1, i+1, value); err != nil {
			return err
		}
	}

	headerStyle, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"E7EEF8"}, Pattern: 1},
	})
	_ = workbook.SetCellStyle(title, "A1", "E1", headerStyle)

	keys := make([]loginKey, 0, len(loads))
	for key := range loads {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].rp == keys[j].rp {
			return keys[i].login < keys[j].login
		}
		return keys[i].rp < keys[j].rp
	})

	row := 2
	for _, key := range keys {
		item := loads[key]
		amount, _ := item.amount.Float64()
		values := []any{key.rp, key.login, item.count, amount, item.iinCount}
		for col, value := range values {
			if err := setCell(workbook, title, row, col+1, value); err != nil {
				return err
			}
		}
		row++
	}
	row++
	_ = setCell(workbook, title, row, 1, "Зафиксировано строк со статусами оплаты")
	_ = setCell(workbook, title, row, 2, fixedCount)
	row++
	_ = setCell(workbook, title, row, 1, "Зафиксировано ИИН со статусами оплаты")
	_ = setCell(workbook, title, row, 2, fixedIINCount)

	for col := 1; col <= 5; col++ {
		name, _ := excelize.ColumnNumberToName(col)
		_ = workbook.SetColWidth(title, name, name, 28)
	}
	return nil
}

func styleAttachColumn(workbook *excelize.File, sheet string, column int) error {
	colName, err := excelize.ColumnNumberToName(column)
	if err != nil {
		return err
	}
	style, _ := workbook.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"DDEFD9"}, Pattern: 1},
	})
	_ = workbook.SetCellStyle(sheet, colName+"1", colName+"1", style)
	width, err := workbook.GetColWidth(sheet, colName)
	if err != nil || width < 18 {
		width = 18
	}
	return workbook.SetColWidth(sheet, colName, colName, width)
}

func getCell(workbook *excelize.File, sheet string, row, col int) string {
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return ""
	}
	value, err := workbook.GetCellValue(sheet, cell)
	if err != nil {
		return ""
	}
	return value
}

func getRowCell(row []string, col int) string {
	index := col - 1
	if index < 0 || index >= len(row) {
		return ""
	}
	return row[index]
}

func setCell(workbook *excelize.File, sheet string, row, col int, value any) error {
	cell, err := excelize.CoordinatesToCellName(col, row)
	if err != nil {
		return err
	}
	return workbook.SetCellValue(sheet, cell, value)
}

func normalizeHeader(value string) string {
	text := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "ё", "е")
	return strings.Join(strings.Fields(text), " ")
}

func normalizeLogin(value string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func normalizeRP(value string) string {
	return normalizeLogin(value)
}

func normalizeStatus(value string) string {
	return normalizeHeader(value)
}

func normalizeIIN(value string) string {
	text := strings.TrimSpace(value)
	if strings.HasSuffix(text, ".0") && onlyDigits(strings.TrimSuffix(text, ".0")) {
		text = strings.TrimSuffix(text, ".0")
	}
	if onlyDigits(text) {
		for len(text) < 12 {
			text = "0" + text
		}
	}
	return text
}

func onlyDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func toDecimal(value string) decimal.Decimal {
	text := strings.ReplaceAll(strings.TrimSpace(value), " ", "")
	text = strings.ReplaceAll(text, ",", ".")
	if text == "" {
		return decimal.Zero
	}
	number, err := decimal.NewFromString(text)
	if err != nil {
		return decimal.Zero
	}
	return number
}

func pyAdd(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Add(right))
}

func pySub(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Sub(right))
}

func pyMul(left, right decimal.Decimal) decimal.Decimal {
	return pyRound(left.Mul(right))
}

func pyDiv(left, right decimal.Decimal) decimal.Decimal {
	if right.IsZero() {
		return decimal.Zero
	}
	return pyRound(left.Div(right))
}

func pyRound(value decimal.Decimal) decimal.Decimal {
	if value.IsZero() {
		return decimal.Zero
	}
	coefficient := strings.TrimPrefix(value.Coefficient().String(), "-")
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		return decimal.Zero
	}
	adjustedExponent := int32(len(coefficient)) + value.Exponent() - 1
	places := int32(28) - adjustedExponent - 1
	return value.RoundBank(places)
}

func makeGroupKey(rp, iin string) string {
	return rp + "\x00" + iin
}

func groupsForRP(groups []*iinGroup, rp string) []*iinGroup {
	var out []*iinGroup
	for _, group := range groups {
		if group.rp == rp {
			out = append(out, group)
		}
	}
	return out
}

func containsLoginKey(items []loginKey, key loginKey) bool {
	for _, item := range items {
		if item == key {
			return true
		}
	}
	return false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
