// ====================================================
// ⚙️  設定
// ====================================================
const API_URL = window.CALCULATOR_API_URL || "http://localhost:8000";

// ====================================================
// 狀態
// ====================================================
const assignedFiles = {}; // { "01": File, "03": File, ... }
let inventoryFile = null;
let onboardNormalFile = null;
let onboardFlyFile = null;
let stockFile = null;
let importFile = null;

const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");
const btnProcess = document.getElementById("btn-process");

// ====================================================
// 初始化 12 個月份格子
// ====================================================
document.querySelectorAll(".month-slot").forEach((slot) => {
  const month = slot.dataset.month;
  const input = slot.querySelector(".slot-input");

  // 點擊格子 → 開啟檔案選擇（排除點到 × 按鈕）
  slot.addEventListener("click", (e) => {
    if (!e.target.classList.contains("remove-btn")) {
      input.click();
    }
  });

  // 選擇檔案後
  input.addEventListener("change", function () {
    if (this.files[0]) assignFile(month, this.files[0]);
    this.value = ""; // 允許重複選同一檔案
  });

  // Drag & Drop
  slot.addEventListener("dragover", (e) => {
    e.preventDefault();
    slot.classList.add("dragover");
  });

  slot.addEventListener("dragleave", () => {
    slot.classList.remove("dragover");
  });

  slot.addEventListener("drop", (e) => {
    e.preventDefault();
    slot.classList.remove("dragover");
    const file = e.dataTransfer.files[0];
    if (file) {
      if (!file.name.match(/\.(xlsx|xls)$/i)) {
        alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
        return;
      }
      assignFile(month, file);
    }
  });
});

// ====================================================
// 指定月份對應檔案
// ====================================================
function assignFile(month, file) {
  assignedFiles[month] = file;
  renderSlot(month);
  updateButton();
}

function removeFile(month) {
  delete assignedFiles[month];
  renderSlot(month);
  updateButton();
}

function renderSlot(month) {
  const slot = document.querySelector(`.month-slot[data-month="${month}"]`);
  const plus = slot.querySelector(".slot-plus");
  let fileEl = slot.querySelector(".slot-filename");
  let removeBtn = slot.querySelector(".remove-btn");

  if (assignedFiles[month]) {
    slot.classList.add("assigned");
    plus.style.display = "none";

    if (!fileEl) {
      fileEl = document.createElement("span");
      fileEl.className = "slot-filename";
      slot.appendChild(fileEl);
    }
    // 截短檔名：最多 14 個字元
    const name = assignedFiles[month].name;
    fileEl.textContent = name.length > 14 ? name.slice(0, 12) + "…" : name;
    fileEl.title = name;

    if (!removeBtn) {
      removeBtn = document.createElement("span");
      removeBtn.className = "remove-btn";
      removeBtn.textContent = "×";
      removeBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        removeFile(month);
      });
      slot.appendChild(removeBtn);
    }
  } else {
    slot.classList.remove("assigned");
    plus.style.display = "";
    if (fileEl) fileEl.remove();
    if (removeBtn) removeBtn.remove();
  }
}

function updateButton() {
  const salesCount = Object.keys(assignedFiles).length;
  const hasInventory = inventoryFile !== null;
  const hasOnboard = onboardNormalFile !== null && onboardFlyFile !== null;
  const hasStock = stockFile !== null;
  const hasImport = importFile !== null;
  const hasAny = salesCount > 0 || hasInventory || onboardNormalFile !== null || onboardFlyFile !== null || hasStock || hasImport;
  btnProcess.disabled = !hasAny;

  const parts = [];
  if (salesCount > 0) {
    const months = Object.keys(assignedFiles).sort();
    parts.push(`銷售明細 ${months.map(m => parseInt(m) + "月").join("、")}`);
  }
  if (hasInventory) parts.push("在途庫存");
  if (hasOnboard) parts.push("機上量");
  else if (onboardNormalFile || onboardFlyFile) parts.push("機上量（需同時上傳兩個檔案）");
  if (hasStock) parts.push("期末存量");
  if (hasImport) parts.push("本月進貨");

  if (parts.length > 0) {
    setStatus("online", `已選取：${parts.join("、")}`);
  } else {
    setStatus("online", "請放入至少一個檔案");
  }
}

// ====================================================
// 機上量上傳
// ====================================================
const onboardNormalDrop = document.getElementById("onboard-normal-drop");
const onboardNormalInput = document.getElementById("onboard-normal-input");
const onboardNormalText = document.getElementById("onboard-normal-text");
const onboardNormalRemove = document.getElementById("onboard-normal-remove");

const onboardFlyDrop = document.getElementById("onboard-fly-drop");
const onboardFlyInput = document.getElementById("onboard-fly-input");
const onboardFlyText = document.getElementById("onboard-fly-text");
const onboardFlyRemove = document.getElementById("onboard-fly-remove");

onboardNormalInput.addEventListener("change", function () {
  if (this.files[0]) setOnboardFile("normal", this.files[0]);
  this.value = "";
});
onboardNormalDrop.addEventListener("dragover", (e) => { e.preventDefault(); onboardNormalDrop.classList.add("dragover"); });
onboardNormalDrop.addEventListener("dragleave", () => { onboardNormalDrop.classList.remove("dragover"); });
onboardNormalDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  onboardNormalDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) { alert("請上傳 Excel 檔案 (.xlsx 或 .xls)"); return; }
    setOnboardFile("normal", file);
  }
});

onboardFlyInput.addEventListener("change", function () {
  if (this.files[0]) setOnboardFile("fly", this.files[0]);
  this.value = "";
});
onboardFlyDrop.addEventListener("dragover", (e) => { e.preventDefault(); onboardFlyDrop.classList.add("dragover"); });
onboardFlyDrop.addEventListener("dragleave", () => { onboardFlyDrop.classList.remove("dragover"); });
onboardFlyDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  onboardFlyDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) { alert("請上傳 Excel 檔案 (.xlsx 或 .xls)"); return; }
    setOnboardFile("fly", file);
  }
});

function setOnboardFile(type, file) {
  if (type === "normal") {
    onboardNormalFile = file;
    onboardNormalDrop.classList.add("assigned");
    const name = file.name.length > 20 ? file.name.slice(0, 18) + "…" : file.name;
    onboardNormalText.textContent = name;
    onboardNormalText.title = file.name;
    onboardNormalRemove.style.display = "";
  } else {
    onboardFlyFile = file;
    onboardFlyDrop.classList.add("assigned");
    const name = file.name.length > 20 ? file.name.slice(0, 18) + "…" : file.name;
    onboardFlyText.textContent = name;
    onboardFlyText.title = file.name;
    onboardFlyRemove.style.display = "";
  }
  updateButton();
}

function removeOnboardFile(type) {
  if (type === "normal") {
    onboardNormalFile = null;
    onboardNormalDrop.classList.remove("assigned");
    onboardNormalText.textContent = "一般航線";
    onboardNormalText.title = "";
    onboardNormalRemove.style.display = "none";
  } else {
    onboardFlyFile = null;
    onboardFlyDrop.classList.remove("assigned");
    onboardFlyText.textContent = "串飛航線";
    onboardFlyText.title = "";
    onboardFlyRemove.style.display = "none";
  }
  updateButton();
}

// ====================================================
// 在途庫存上傳
// ====================================================
const inventoryDrop = document.getElementById("inventory-drop");
const inventoryInput = document.getElementById("inventory-input");
const invText = document.getElementById("inv-text");
const invRemove = document.getElementById("inv-remove");

inventoryDrop.addEventListener("click", (e) => {
  if (!e.target.classList.contains("inv-remove")) inventoryInput.click();
});

inventoryInput.addEventListener("change", function () {
  if (this.files[0]) setInventoryFile(this.files[0]);
  this.value = "";
});

inventoryDrop.addEventListener("dragover", (e) => {
  e.preventDefault();
  inventoryDrop.classList.add("dragover");
});

inventoryDrop.addEventListener("dragleave", () => {
  inventoryDrop.classList.remove("dragover");
});

inventoryDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  inventoryDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) {
      alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
      return;
    }
    setInventoryFile(file);
  }
});

function setInventoryFile(file) {
  inventoryFile = file;
  inventoryDrop.classList.add("assigned");
  const name = file.name.length > 30 ? file.name.slice(0, 28) + "…" : file.name;
  invText.textContent = name;
  invText.title = file.name;
  invRemove.style.display = "";
  updateButton();
}

function removeInventoryFile() {
  inventoryFile = null;
  inventoryDrop.classList.remove("assigned");
  invText.textContent = "點擊或拖曳採購未交量 Excel 檔案";
  invText.title = "";
  invRemove.style.display = "none";
  updateButton();
}

// ====================================================
// 期末存量上傳
// ====================================================
const stockDrop = document.getElementById("stock-drop");
const stockInput = document.getElementById("stock-input");
const stockText = document.getElementById("stock-text");
const stockRemove = document.getElementById("stock-remove");

stockDrop.addEventListener("click", (e) => {
  if (!e.target.classList.contains("inv-remove")) stockInput.click();
});

stockInput.addEventListener("change", function () {
  if (this.files[0]) setStockFile(this.files[0]);
  this.value = "";
});

stockDrop.addEventListener("dragover", (e) => {
  e.preventDefault();
  stockDrop.classList.add("dragover");
});

stockDrop.addEventListener("dragleave", () => {
  stockDrop.classList.remove("dragover");
});

stockDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  stockDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) {
      alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
      return;
    }
    setStockFile(file);
  }
});

function setStockFile(file) {
  stockFile = file;
  stockDrop.classList.add("assigned");
  const name = file.name.length > 30 ? file.name.slice(0, 28) + "…" : file.name;
  stockText.textContent = name;
  stockText.title = file.name;
  stockRemove.style.display = "";
  updateButton();
}

function removeStockFile() {
  stockFile = null;
  stockDrop.classList.remove("assigned");
  stockText.textContent = "選擇 IT Inventory Excel 檔案";
  stockText.title = "";
  stockRemove.style.display = "none";
  updateButton();
}

// ====================================================
// 本月進貨上傳
// ====================================================
const importDrop = document.getElementById("import-drop");
const importInput = document.getElementById("import-input");
const importText = document.getElementById("import-text");
const importRemove = document.getElementById("import-remove");

importDrop.addEventListener("click", (e) => {
  if (!e.target.classList.contains("inv-remove")) importInput.click();
});

importInput.addEventListener("change", function () {
  if (this.files[0]) setImportFile(this.files[0]);
  this.value = "";
});

importDrop.addEventListener("dragover", (e) => {
  e.preventDefault();
  importDrop.classList.add("dragover");
});

importDrop.addEventListener("dragleave", () => {
  importDrop.classList.remove("dragover");
});

importDrop.addEventListener("drop", (e) => {
  e.preventDefault();
  importDrop.classList.remove("dragover");
  const file = e.dataTransfer.files[0];
  if (file) {
    if (!file.name.match(/\.(xlsx|xls)$/i)) {
      alert("請上傳 Excel 檔案 (.xlsx 或 .xls)");
      return;
    }
    setImportFile(file);
  }
});

function setImportFile(file) {
  importFile = file;
  importDrop.classList.add("assigned");
  const name = file.name.length > 30 ? file.name.slice(0, 28) + "…" : file.name;
  importText.textContent = name;
  importText.title = file.name;
  importRemove.style.display = "";
  updateButton();
}

function removeImportFile() {
  importFile = null;
  importDrop.classList.remove("assigned");
  importText.textContent = "選擇進貨明細 Excel 檔案";
  importText.title = "";
  importRemove.style.display = "none";
  updateButton();
}

// ====================================================
// API 呼叫
// ====================================================
async function processFile() {
  const entries = Object.entries(assignedFiles).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length === 0 && !inventoryFile && !onboardNormalFile && !onboardFlyFile && !stockFile && !importFile) return;

  setLoading(true);
  setStatus("loading", "處理中，請稍候…");

  const formData = new FormData();
  entries.forEach(([month, file]) => {
    formData.append("files", file);
    formData.append("months", month);
  });

  if (inventoryFile) {
    formData.append("inventory_file", inventoryFile);
  }

  if (onboardNormalFile && onboardFlyFile) {
    formData.append("onboard_normal_file", onboardNormalFile);
    formData.append("onboard_fly_file", onboardFlyFile);
  } else if (onboardNormalFile || onboardFlyFile) {
    alert("機上量需同時上傳一般航線與串飛航線兩個檔案");
    setLoading(false);
    return;
  }

  if (stockFile) {
    formData.append("stock_file", stockFile);
  }

  if (importFile) {
    formData.append("import_file", importFile);
  }

  try {
    const response = await fetch(`${API_URL}/process-excel`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      let errorDetail = "檔案處理失敗";
      try {
        const errJson = await response.json();
        errorDetail = errJson.detail || errorDetail;
      } catch (e) {
        console.error(e);
      }
      alert(`錯誤：${errorDetail}`);
      setStatus("error", "處理失敗");
      return;
    }

    const contentDisposition = response.headers.get("Content-Disposition");
    let filename = "TTW sales summary.xlsx";
    if (contentDisposition) {
      const m = contentDisposition.match(/filename="([^"]+)"/);
      if (m) filename = m[1];
    }

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);

    setStatus("online", "處理成功，開始下載 🎉");
  } catch (err) {
    alert("無法連接到伺服器或發生網路錯誤");
    setStatus("error", "連線失敗");
    console.error(err);
  } finally {
    setLoading(false);
  }
}

// ====================================================
// 輔助函式
// ====================================================
function setLoading(isLoading) {
  btnProcess.classList.toggle("loading", isLoading);
}

function setStatus(state, text) {
  statusDot.className = "status-dot " + state;
  statusText.textContent = text;
}

async function checkHealth() {
  setStatus("loading", "連線中…");
  try {
    const res = await fetch(`${API_URL}/`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      setStatus("online", "API 已連線，請將檔案拖入月份格子");
    } else {
      setStatus("error", "API 伺服器異常");
    }
  } catch {
    setStatus("error", "無法連接後端");
  }
}

checkHealth();
