# 飛買家銷售資料彙整工具

一個全端網頁應用程式，用於自動彙整電商平台的 Excel 銷售明細，依 SKU 加總銷售量與銷售額，並匯出彙整後的報表。
網站：https://zeabur-test1.zeabur.app/

## 功能介紹

- **上傳 Excel 銷售明細** — 支援拖曳或點擊上傳 `.xlsx` / `.xls` 檔案
- **自動彙整資料** — 依 SKU 分組，加總各品項的銷售量與銷售額
- **自動偵測月份** — 從檔名（如 `20260301-20260327`）自動判斷月份，輸出對應欄位名稱（如「三月銷售量」）
- **即時下載報表** — 處理完成後自動下載彙整結果 Excel 檔

## 專案架構

```
zeabur/
├── backend/
│   ├── main.py           # FastAPI 後端，負責 Excel 資料處理
│   ├── requirements.txt  # Python 套件相依
│   └── zbpack.json       # Zeabur 部署設定
├── frontend/
│   ├── index.html        # 主頁面
│   ├── app.js            # 前端邏輯（上傳、呼叫 API、下載）
│   ├── style.css         # UI 樣式
│   └── zbpack.json       # Zeabur 前端部署設定
└── preporcess.py         # 原始獨立處理腳本（參考用）
```

## 技術棧

| 層級 | 技術 |
|------|------|
| 後端 | Python、FastAPI、pandas、openpyxl |
| 前端 | HTML5、CSS3、Vanilla JavaScript |
| 部署 | Zeabur |

## 使用方式

### 輸入檔案格式

Excel 檔案需包含以下欄位：

| 欄位 | 說明 |
|------|------|
| `SKU no` | 商品 SKU 編號 |
| `SKU title` | 商品名稱 |
| `Volume` | 銷售量 |
| `Amount` | 銷售額 |

若 Excel 內有多個工作表，程式會自動尋找名稱包含 `details` 的工作表。

### 輸出檔案格式

| 欄位 | 說明 |
|------|------|
| SKU No. | 正規化後的 SKU 編號 |
| 品名 | 商品名稱 |
| {月份}銷售量 | 當月銷售量合計 |
| {月份}銷售額 | 當月銷售額合計 |

## API

| 方法 | 路徑 | 說明 |
|------|------|------|
| `GET` | `/` | 健康檢查 |
| `POST` | `/process-excel` | 上傳 Excel 並回傳彙整結果 |

## 本地開發

```bash
# 安裝後端相依套件
cd backend
pip install -r requirements.txt

# 啟動後端（port 8080）
uvicorn main:app --host 0.0.0.0 --port 8080 --reload
```

前端為靜態檔案，直接用瀏覽器開啟 `frontend/index.html`，或以任意靜態伺服器提供服務即可。
