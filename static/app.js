const authPanel = document.querySelector('#authPanel');
const loginForm = document.querySelector('#loginForm');
const passwordInput = document.querySelector('#passwordInput');
const loginError = document.querySelector('#loginError');
const appShell = document.querySelector('#appShell');
const logoutButton = document.querySelector('#logoutButton');
const form = document.querySelector('#uploadForm');
const fileInput = document.querySelector('#fileInput');
const fileName = document.querySelector('#fileName');
const jobIdInput = document.querySelector('#jobId');
const socketStatus = document.querySelector('#socketStatus');
const progressText = document.querySelector('#progressText');
const progressPercent = document.querySelector('#progressPercent');
const progressFill = document.querySelector('#progressFill');
const progressLog = document.querySelector('#progressLog');
const errorText = document.querySelector('#errorText');
const downloadLink = document.querySelector('#downloadLink');
const submitButton = document.querySelector('#submitButton');
const cancelButton = document.querySelector('#cancelButton');
const processTitle = document.querySelector('#processTitle');
const processHint = document.querySelector('#processHint');
const processInputs = [...document.querySelectorAll('input[name="process"]')];
const historyList = document.querySelector('#historyList');
const clearHistoryButton = document.querySelector('#clearHistoryButton');

const processes = {
  rotation: {
    endpoint: '/rotate/',
    title: 'Ротация',
    button: 'Запустить ротацию',
    download: 'rotation_result.xlsx',
    start: 'Отправляю файл для ротации',
    failed: 'Ротация не завершена',
    accepted: 'Файл принят. Ротация выполняется.',
    done: 'Ротация завершена, файл можно скачать',
    hint: 'Материалы будут распределены по колонке «Закрепить».',
  },
  alignment: {
    endpoint: '/balance/',
    title: 'Выравнивание',
    button: 'Запустить выравнивание',
    download: 'alignment_result.xlsx',
    start: 'Отправляю файл для выравнивания',
    failed: 'Выравнивание не завершено',
    accepted: 'Файл принят. Выравнивание выполняется.',
    done: 'Выравнивание завершено, файл можно скачать',
    hint: 'Нагрузка выравнивается по ИИН и сумме внутри каждого РП.',
  },
};

let socket;
let activeJobId = localStorage.getItem('activeJobId') || '';

function currentProcessKey() {
  return processInputs.find((input) => input.checked)?.value || 'rotation';
}

function currentProcess() {
  return processes[currentProcessKey()];
}

function createJobId() {
  if (crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function requestJSON(url, options = {}) {
  const response = await fetch(url, {
    credentials: 'same-origin',
    ...options,
    headers: {
      ...(options.body && !(options.body instanceof FormData) ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers || {}),
    },
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ error: 'Ошибка запроса.' }));
    throw new Error(payload.error || 'Ошибка запроса.');
  }
  return response.json();
}

function showAuthenticated(authenticated) {
  authPanel.classList.toggle('hidden', authenticated);
  appShell.classList.toggle('hidden', !authenticated);
}

async function checkAuth() {
  const payload = await requestJSON('/api/me');
  showAuthenticated(payload.authenticated);
  if (payload.authenticated) {
    await refreshHistory();
    if (activeJobId) {
      await restoreJob(activeJobId);
    } else {
      prepareNewJob();
    }
  }
}

function connectProgress(jobId) {
  if (socket) {
    socket.close();
  }
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  socket = new WebSocket(`${protocol}://${window.location.host}/ws/rotation/${jobId}/`);

  socket.addEventListener('open', () => {
    socketStatus.textContent = 'WebSocket подключен';
    socketStatus.classList.add('is-online');
  });

  socket.addEventListener('close', () => {
    socketStatus.textContent = 'WebSocket отключен';
    socketStatus.classList.remove('is-online');
  });

  socket.addEventListener('message', (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'progress') {
      setProgress(data.percent, data.message);
      appendProgressLog(data.percent, data.message);
      if (data.state && data.state !== 'running') {
        submitButton.disabled = false;
        cancelButton.disabled = true;
      }
    }
    if (data.type === 'job_ready') {
      downloadLink.href = data.download_url;
      downloadLink.download = currentProcess().download;
      downloadLink.classList.remove('hidden');
      cancelButton.disabled = true;
      submitButton.disabled = false;
      localStorage.removeItem('activeJobId');
      setProgress(100, data.message || 'Файл готов.');
      appendProgressLog(100, data.message || 'Файл готов.');
      refreshHistory();
    }
    if (data.type === 'job_error' || data.type === 'job_canceled') {
      errorText.textContent = data.message || 'Задача остановлена.';
      cancelButton.disabled = true;
      submitButton.disabled = false;
      localStorage.removeItem('activeJobId');
      appendProgressLog(0, errorText.textContent);
      refreshHistory();
    }
  });
}

function setProgress(percent, message) {
  const value = Math.max(0, Math.min(100, Number(percent) || 0));
  progressFill.style.width = `${value}%`;
  progressPercent.textContent = `${value}%`;
  progressText.textContent = message || 'Обработка';
}

function appendProgressLog(percent, message) {
  if (!message) {
    return;
  }

  const value = Math.max(0, Math.min(100, Number(percent) || 0));
  const previousMessage = progressLog.lastElementChild?.dataset.message;
  if (previousMessage === message) {
    return;
  }

  const item = document.createElement('li');
  item.dataset.message = message;
  item.innerHTML = `<b>${value}%</b><span></span>`;
  item.querySelector('span').textContent = message;
  progressLog.append(item);
  progressLog.scrollTop = progressLog.scrollHeight;
}

function resetProgressLog() {
  progressLog.replaceChildren();
}

function resetResult() {
  errorText.textContent = '';
  downloadLink.classList.add('hidden');
  if (downloadLink.href && downloadLink.href.startsWith('blob:')) {
    URL.revokeObjectURL(downloadLink.href);
  }
}

function updateProcessView() {
  const process = currentProcess();
  processTitle.textContent = process.title;
  submitButton.textContent = process.button;
  processHint.textContent = process.hint;
}

function prepareNewJob() {
  const jobId = createJobId();
  activeJobId = jobId;
  jobIdInput.value = jobId;
  connectProgress(jobId);
  cancelButton.disabled = true;
}

async function restoreJob(jobId) {
  try {
    const job = await requestJSON(`/api/jobs/${jobId}`);
    jobIdInput.value = job.id;
    activeJobId = job.id;
    connectProgress(job.id);
    renderJob(job);
    if (job.state === 'running') {
      submitButton.disabled = true;
      cancelButton.disabled = false;
      localStorage.setItem('activeJobId', job.id);
    } else {
      submitButton.disabled = false;
      cancelButton.disabled = true;
      localStorage.removeItem('activeJobId');
    }
  } catch (error) {
    localStorage.removeItem('activeJobId');
    prepareNewJob();
  }
}

function renderJob(job) {
  resetResult();
  resetProgressLog();
  setProgress(job.percent, job.message);
  for (const item of job.log || []) {
    appendProgressLog(item.percent, item.message);
  }
  if (job.state === 'ready') {
    downloadLink.href = job.download_url;
    downloadLink.download = job.filename || currentProcess().download;
    downloadLink.classList.remove('hidden');
  }
  if (job.state === 'error' || job.state === 'canceled') {
    errorText.textContent = job.error || job.message;
  }
}

function formatDate(value) {
  if (!value || value.startsWith('0001-')) {
    return '';
  }
  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value));
}

async function refreshHistory() {
  const jobs = await requestJSON('/api/jobs');
  historyList.replaceChildren();
  clearHistoryButton.disabled = jobs.length === 0;
  if (!jobs.length) {
    const empty = document.createElement('li');
    empty.className = 'history-empty';
    empty.textContent = 'Истории пока нет';
    historyList.append(empty);
    return;
  }

  for (const job of jobs) {
    const item = document.createElement('li');
    item.className = 'history-item';
    const title = document.createElement('button');
    title.type = 'button';
    title.textContent = `${processes[job.process]?.title || job.process} · ${statusText(job.state)} · ${job.percent}%`;
    title.addEventListener('click', () => restoreJob(job.id));

    const meta = document.createElement('span');
    const completed = formatDate(job.completed_at);
    meta.textContent = `Запущен: ${formatDate(job.started_at || job.created_at)}${completed ? ` · Завершен: ${completed}` : ''}`;

    item.append(title, meta);
    if (job.download_url) {
      const link = document.createElement('a');
      link.href = job.download_url;
      link.textContent = 'Скачать';
      item.append(link);
    }
    historyList.append(item);
  }
}

async function clearHistory() {
  if (!confirm('Очистить всю историю и удалить все XLSX результаты?')) {
    return;
  }
  clearHistoryButton.disabled = true;
  await requestJSON('/api/history/clear', { method: 'POST' });
  localStorage.removeItem('activeJobId');
  activeJobId = '';
  resetResult();
  resetProgressLog();
  setProgress(0, 'Ожидание файла');
  submitButton.disabled = false;
  cancelButton.disabled = true;
  await refreshHistory();
  prepareNewJob();
}

function statusText(state) {
  return {
    running: 'в работе',
    ready: 'готово',
    error: 'ошибка',
    canceled: 'отменено',
  }[state] || state;
}

loginForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  loginError.textContent = '';
  try {
    await requestJSON('/api/login', {
      method: 'POST',
      body: JSON.stringify({ password: passwordInput.value }),
    });
    passwordInput.value = '';
    showAuthenticated(true);
    await refreshHistory();
    prepareNewJob();
  } catch (error) {
    loginError.textContent = error.message;
  }
});

logoutButton.addEventListener('click', async () => {
  await requestJSON('/api/logout', { method: 'POST' });
  localStorage.removeItem('activeJobId');
  showAuthenticated(false);
});

fileInput.addEventListener('change', () => {
  fileName.textContent = fileInput.files[0]?.name || 'Выберите файл';
});

processInputs.forEach((input) => {
  input.addEventListener('change', updateProcessView);
});

cancelButton.addEventListener('click', async () => {
  if (!activeJobId) {
    return;
  }
  cancelButton.disabled = true;
  await requestJSON(`/api/jobs/${activeJobId}/cancel`, { method: 'POST' });
  setProgress(progressPercent.textContent.replace('%', ''), 'Отменяю задачу...');
});

clearHistoryButton.addEventListener('click', async () => {
  try {
    await clearHistory();
  } catch (error) {
    errorText.textContent = error.message || 'Не удалось очистить историю.';
    await refreshHistory();
  }
});

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const process = currentProcess();
  prepareNewJob();
  resetResult();
  resetProgressLog();
  setProgress(1, process.start);
  appendProgressLog(1, process.start);

  submitButton.disabled = true;
  cancelButton.disabled = false;
  localStorage.setItem('activeJobId', activeJobId);

  try {
    const response = await fetch(process.endpoint, {
      method: 'POST',
      body: new FormData(form),
      credentials: 'same-origin',
    });

    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: 'Ошибка обработки файла.' }));
      throw new Error(payload.error || 'Ошибка обработки файла.');
    }

    const payload = await response.json();
    activeJobId = payload.job_id || activeJobId;
    localStorage.setItem('activeJobId', activeJobId);
    setProgress(2, process.accepted);
    appendProgressLog(2, process.accepted);
    await refreshHistory();
  } catch (error) {
    errorText.textContent = error.message || 'Не удалось отправить файл на сервер.';
    setProgress(0, process.failed);
    appendProgressLog(0, errorText.textContent);
    submitButton.disabled = false;
    cancelButton.disabled = true;
    localStorage.removeItem('activeJobId');
  }
});

document.addEventListener('DOMContentLoaded', async () => {
  updateProcessView();
  try {
    await checkAuth();
  } catch (error) {
    showAuthenticated(false);
  }
});
