// ====================================================
// ⚙️  設定：部署到 Zeabur 後，把這裡改成你的後端 Domain
// 本地測試時保持 http://localhost:8000
// ====================================================
const API_URL = window.CALCULATOR_API_URL || "http://localhost:8000";

// ====================================================
// DOM 元素
// ====================================================
const expressionDisplay = document.getElementById("expression-display");
const resultDisplay = document.getElementById("result-display");
const equalsBtn = document.getElementById("btn-equals");
const statusDot = document.getElementById("status-dot");
const statusText = document.getElementById("status-text");

// ====================================================
// 狀態
// ====================================================
let currentExpression = "";
let lastWasResult = false;

// ====================================================
// 顯示更新
// ====================================================
function updateDisplay(expr, result = null, isError = false) {
  expressionDisplay.textContent = expr || "0";

  if (result !== null) {
    resultDisplay.textContent = result;
    resultDisplay.className = isError ? "error" : "is-result";
  } else {
    resultDisplay.textContent = expr || "0";
    resultDisplay.className = "";
  }
}

// ====================================================
// 按鈕點擊邏輯
// ====================================================
function handleInput(value) {
  // 若上一次是計算結果，且繼續輸入數字，則清空重新開始
  if (lastWasResult && /[\d.]/.test(value)) {
    currentExpression = "";
    lastWasResult = false;
  } else if (lastWasResult && /[+\-×÷%^]/.test(value)) {
    // 沿用上次結果繼續計算
    lastWasResult = false;
  }

  // 顯示用符號 → API 用符號的轉換
  const displayMap = { "×": "*", "÷": "/" };
  const apiValue = displayMap[value] || value;

  // 防止連續輸入兩個運算符
  if (/[+\-*/%^]/.test(apiValue)) {
    if (/[+\-*/%^]$/.test(currentExpression)) {
      currentExpression = currentExpression.slice(0, -1);
    }
  }

  currentExpression += apiValue;
  updateDisplay(formatExpression(currentExpression));
}

function handleClear() {
  currentExpression = "";
  lastWasResult = false;
  updateDisplay("", null);
  resultDisplay.textContent = "0";
  resultDisplay.className = "";
  expressionDisplay.textContent = "";
}

function handleBackspace() {
  if (lastWasResult) {
    handleClear();
    return;
  }
  currentExpression = currentExpression.slice(0, -1);
  updateDisplay(formatExpression(currentExpression));
}

function handleDecimal() {
  // 找到最後一個數字段，確保不重複加小數點
  const parts = currentExpression.split(/[+\-*/%()\^]/);
  const lastPart = parts[parts.length - 1];
  if (!lastPart.includes(".")) {
    if (lastPart === "" || currentExpression === "") {
      currentExpression += "0";
    }
    currentExpression += ".";
    updateDisplay(formatExpression(currentExpression));
  }
}

// 格式化顯示（把 * 和 / 換回美觀符號）
function formatExpression(expr) {
  return expr.replace(/\*/g, "×").replace(/\//g, "÷");
}

// ====================================================
// 呼叫後端 API 計算
// ====================================================
async function calculate() {
  if (!currentExpression.trim()) return;

  setLoading(true);
  setStatus("loading", "計算中…");

  try {
    const response = await fetch(`${API_URL}/calculate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ expression: currentExpression }),
    });

    const data = await response.json();

    if (!response.ok) {
      // 後端回傳錯誤（400 等）
      updateDisplay(formatExpression(currentExpression), data.detail || "錯誤", true);
      setStatus("error", "計算失敗");
    } else {
      const formatted = formatNumber(data.result);
      expressionDisplay.textContent = formatExpression(currentExpression) + " =";
      resultDisplay.textContent = formatted;
      resultDisplay.className = "is-result";
      currentExpression = String(data.result);
      lastWasResult = true;
      setStatus("online", "API 連線正常");
    }
  } catch (err) {
    updateDisplay(formatExpression(currentExpression), "無法連接到後端 API", true);
    setStatus("error", "連線失敗");
    console.error("API 錯誤：", err);
  } finally {
    setLoading(false);
  }
}

// ====================================================
// 輔助函式
// ====================================================
function formatNumber(num) {
  // 顯示最多 10 位有效數字，避免浮點誤差
  const n = parseFloat(num);
  if (isNaN(n)) return String(num);
  if (Number.isInteger(n)) return n.toLocaleString();
  return parseFloat(n.toPrecision(12)).toLocaleString(undefined, {
    maximumFractionDigits: 10,
  });
}

function setLoading(isLoading) {
  if (isLoading) {
    equalsBtn.classList.add("loading");
  } else {
    equalsBtn.classList.remove("loading");
  }
}

function setStatus(state, text) {
  statusDot.className = "status-dot " + state;
  statusText.textContent = text;
}

// ====================================================
// 鍵盤支援
// ====================================================
document.addEventListener("keydown", (e) => {
  if (e.key >= "0" && e.key <= "9") handleInput(e.key);
  else if (e.key === "+") handleInput("+");
  else if (e.key === "-") handleInput("-");
  else if (e.key === "*") handleInput("×");
  else if (e.key === "/") { e.preventDefault(); handleInput("÷"); }
  else if (e.key === "%") handleInput("%");
  else if (e.key === "^") handleInput("^");
  else if (e.key === ".") handleDecimal();
  else if (e.key === "Enter" || e.key === "=") calculate();
  else if (e.key === "Backspace") handleBackspace();
  else if (e.key === "Escape") handleClear();
  else if (e.key === "(") handleInput("(");
  else if (e.key === ")") handleInput(")");
});

// ====================================================
// Ripple 特效
// ====================================================
document.querySelectorAll(".btn").forEach((btn) => {
  btn.addEventListener("click", function (e) {
    const ripple = document.createElement("span");
    ripple.className = "ripple";
    const rect = this.getBoundingClientRect();
    const size = Math.max(rect.width, rect.height);
    ripple.style.width = ripple.style.height = size + "px";
    ripple.style.left = e.clientX - rect.left - size / 2 + "px";
    ripple.style.top = e.clientY - rect.top - size / 2 + "px";
    this.appendChild(ripple);
    setTimeout(() => ripple.remove(), 500);
  });
});

// ====================================================
// 初始化：健康檢查
// ====================================================
async function checkHealth() {
  setStatus("loading", "連線中…");
  try {
    const res = await fetch(`${API_URL}/`, { signal: AbortSignal.timeout(5000) });
    if (res.ok) {
      setStatus("online", "API 連線正常");
    } else {
      setStatus("error", "API 回應異常");
    }
  } catch {
    setStatus("error", "無法連接後端");
  }
}

checkHealth();
