const taskExamples = {
  add: { a: 21, b: 21 },
  factorial: { number: 20 },
  sha256: { text: "MiniFlow" },
  sleep: { seconds: 2 },
  unstable_demo: { message: "Simulated downstream failure" },
  word_count: { text: "Reliable systems expect failure and recover from it." },
};

const form = document.querySelector("#task-form");
const nameInput = document.querySelector("#task-name");
const paramsInput = document.querySelector("#task-params");
const message = document.querySelector("#form-message");
const table = document.querySelector("#task-table");
const empty = document.querySelector("#empty");
const dialog = document.querySelector("#task-dialog");

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, char => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#039;", '"': "&quot;"
  })[char]);
}

async function loadMetrics() {
  const data = await fetch("/api/metrics").then(response => response.json());
  const cards = [
    ["Total tasks", data.total],
    ["Running", data.by_status.running],
    ["Success rate", data.success_rate === null ? "—" : `${data.success_rate}%`],
    ["Avg duration", data.average_duration_ms === null ? "—" : `${data.average_duration_ms}ms`],
  ];
  document.querySelector("#metrics").innerHTML = cards.map(([label, value]) =>
    `<article class="metric"><small>${label}</small><strong>${value}</strong></article>`
  ).join("");

  if (!nameInput.options.length) {
    data.registered_tasks.forEach(name => nameInput.add(new Option(name, name)));
    nameInput.value = "add";
  }
}

async function loadTasks() {
  const tasks = await fetch("/api/tasks?limit=50").then(response => response.json());
  empty.hidden = tasks.length > 0;
  table.innerHTML = tasks.map(task => `
    <tr data-id="${escapeHtml(task.id)}">
      <td><span class="task-name">${escapeHtml(task.name)}</span><span class="task-id">${task.id.slice(0, 10)}</span></td>
      <td><span class="status ${task.status}">${task.status}</span></td>
      <td>${task.priority}</td><td>${task.attempts}/${task.max_retries + 1}</td>
      <td>${task.duration_ms === null ? "—" : `${task.duration_ms} ms`}</td>
      <td>${new Date(task.created_at).toLocaleTimeString()}</td>
    </tr>`).join("");
  table.querySelectorAll("tr").forEach(row => row.addEventListener("click", () => showTask(row.dataset.id)));
}

async function showTask(id) {
  const task = await fetch(`/api/tasks/${id}`).then(response => response.json());
  document.querySelector("#dialog-title").textContent = `${task.name} · ${task.status}`;
  document.querySelector("#dialog-content").textContent = JSON.stringify(task, null, 2);
  dialog.showModal();
}

async function refresh() {
  try { await Promise.all([loadMetrics(), loadTasks()]); }
  catch (error) { console.error("Dashboard refresh failed", error); }
}

nameInput.addEventListener("change", () => {
  paramsInput.value = JSON.stringify(taskExamples[nameInput.value] || {}, null, 2);
});

form.addEventListener("submit", async event => {
  event.preventDefault();
  message.textContent = "";
  try {
    const response = await fetch("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: nameInput.value,
        params: JSON.parse(paramsInput.value),
        priority: Number(document.querySelector("#priority").value),
        max_retries: Number(document.querySelector("#retries").value),
        delay_seconds: Number(document.querySelector("#delay").value),
      }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || "Request failed");
    message.textContent = `Queued ${data.id.slice(0, 12)}…`;
    await refresh();
  } catch (error) { message.textContent = error.message; }
});

document.querySelector("#refresh").addEventListener("click", refresh);
document.querySelector("#close-dialog").addEventListener("click", () => dialog.close());
refresh();
setInterval(refresh, 1500);
