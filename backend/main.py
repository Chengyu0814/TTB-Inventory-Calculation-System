import io
import json
from urllib.parse import quote
from typing import List, Optional
from functools import reduce

import pandas as pd
from fastapi import FastAPI, Form, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="Excel Processor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def health_check():
    return {"status": "ok", "message": "Excel Processor API is running 🚀"}


MONTH_NAMES = {
    "01": "一月", "02": "二月", "03": "三月", "04": "四月",
    "05": "五月", "06": "六月", "07": "七月", "08": "八月",
    "09": "九月", "10": "十月", "11": "十一月", "12": "十二月"
}

MONTH_ORDER = [
    "一月", "二月", "三月", "四月", "五月", "六月",
    "七月", "八月", "九月", "十月", "十一月", "十二月"
]

MONTH_NUM = {v: k for k, v in MONTH_NAMES.items()}


async def process_inventory(file: UploadFile) -> pd.DataFrame:
    """處理 採購未交量 檔案，回傳 SKU No. + 品名 + 在途庫存 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=3, dtype={"品    號": str})
        df = df[["品    號", "品   名", "未交數量", "交貨庫"]].rename(columns={
            "品    號": "SKU No.",
            "品   名": "品名",
            "未交數量": "在途庫存"
        })
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"在途庫存檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取在途庫存檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5)
    df = df.dropna(subset=["SKU No."])
    df = df[~df["SKU No."].str.contains(r'[\u4e00-\u9fff]', regex=True)]
    df = df[df["交貨庫"] == "華膳-IT"]
    df = df[df["SKU No."].str.endswith("A", na=False)]
    df["SKU No."] = df["SKU No."].str.extract(r'(.{5})A$', expand=False)
    df["SKU No."] = df["SKU No."].astype(str).str.strip()

    df_res = df.groupby("SKU No.", as_index=False).agg({
        "品名": "first",
        "在途庫存": "sum"
    })
    return df_res[["SKU No.", "品名", "在途庫存"]]


async def process_cost(file: UploadFile, exchange_rates: dict) -> pd.DataFrame:
    """處理 品號價格資料 檔案，回傳 SKU No. + 品名 + TWD成本 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2, dtype={"品號": str})
        df = df[["品號", "品名", "幣別名稱", "採購單價", "核價日"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"商品成本檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取商品成本檔案失敗: {str(e)}")

    df["品號"] = df["品號"].astype(str).str.strip()
    df = df[df["品號"].str.endswith("A", na=False)]
    df["品號"] = df["品號"].str.extract(r'(.{5})A$', expand=False)

    exchange_rates["台幣"] = 1.0
    df["核價日"] = pd.to_datetime(df["核價日"])
    df = df.sort_values(by=["品號", "核價日"], ascending=[True, True])
    df = df.drop_duplicates(subset=["品號"], keep="last")

    df["TWD成本"] = (df["幣別名稱"].map(exchange_rates) * df["採購單價"]).round(0)
    df = df.reset_index(drop=True)
    df.rename(columns={"品號": "SKU No."}, inplace=True)
    return df[["SKU No.", "品名", "TWD成本"]]


async def process_import(file: UploadFile) -> pd.DataFrame:
    """處理 進貨明細 檔案，回傳 SKU No. + 品名 + 本月進貨 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=3, dtype={"品號": str})
        df = df[["品號", "品名", "驗收數量", "庫別"]].rename(columns={"品號": "SKU No.", "驗收數量": "本月進貨"})
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"本月進貨檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取本月進貨檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5)
    df = df[df["庫別"] == "華膳-IT"]
    df = df[df["SKU No."].str.endswith("A", na=False)]
    df["SKU No."] = df["SKU No."].str.extract(r'(.{5})A$', expand=False)
    df["SKU No."] = df["SKU No."].astype(str).str.strip()

    df_grouped = df.groupby("SKU No.")["本月進貨"].sum().reset_index()
    names = df.drop_duplicates(subset=["SKU No."], keep="first")[["SKU No.", "品名"]]
    df_res = df_grouped.merge(names, on="SKU No.", how="left")
    return df_res[["SKU No.", "品名", "本月進貨"]]


async def process_stock(file: UploadFile) -> pd.DataFrame:
    """處理 期末存量 檔案，回傳 SKU No. + 品名 + 期末存量 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=5, dtype={"料號": str})
        df = df[["料號", "品名", "期末存量"]].rename(columns={"料號": "SKU No."})
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"期末存量檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取期末存量檔案失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.zfill(5).str.strip()
    first_empty = df[df["SKU No."].isnull()].index.min()
    if pd.notna(first_empty):
        df = df.iloc[:first_empty]
    return df[["SKU No.", "品名", "期末存量"]]


async def process_onboard(normal_file: UploadFile, fly_file: UploadFile) -> pd.DataFrame:
    """處理 機上量 檔案（一般航線 × 41，串飛航線 × 10），回傳 SKU No. + 品名 + 機上量 的 DataFrame"""

    async def read_onboard_file(file: UploadFile, multiplier: int):
        contents = await file.read()
        try:
            df = pd.read_excel(io.BytesIO(contents), header=1, dtype={"SKU No.": str})
            df = df.dropna(subset=["SKU No."]).copy()
            df["SKU No."] = df["SKU No."].astype(str).str.strip().str.zfill(5)
            df_calc = df[["SKU No.", "數量"]].copy()
            df_calc["裝載數量"] = df_calc["數量"] * multiplier
            df_result = df_calc.groupby("SKU No.")["裝載數量"].sum().reset_index()
            df_unique = df[["SKU No.", "DESCRIPTION"]].drop_duplicates(subset=["SKU No."])
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"機上量檔案 '{file.filename}' 缺少必要欄位：{str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"讀取機上量檔案 '{file.filename}' 失敗: {str(e)}")
        return df_result, df_unique

    df_normal, normal_unique = await read_onboard_file(normal_file, 41)
    df_fly, fly_unique = await read_onboard_file(fly_file, 10)

    df_merged = pd.merge(df_normal, df_fly, on="SKU No.", how="outer", suffixes=("_normal", "_fly"))
    df_merged["機上量"] = df_merged["裝載數量_normal"].fillna(0) + df_merged["裝載數量_fly"].fillna(0)
    df_merged = df_merged[["SKU No.", "機上量"]]
    df_merged = df_merged.merge(normal_unique, on="SKU No.", how="left")
    df_merged = df_merged.merge(fly_unique, on="SKU No.", how="left", suffixes=("", "_fly"))
    df_merged["品名"] = df_merged["DESCRIPTION"].fillna(df_merged["DESCRIPTION_fly"])
    return df_merged[["SKU No.", "品名", "機上量"]]


@app.post("/scan-cost-currencies") #處理幣別，匯率轉換
async def scan_cost_currencies(cost_file: UploadFile = File(...)):
    contents = await cost_file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2, dtype={"品號": str})
        df = df[["品號", "幣別名稱"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"商品成本檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取商品成本檔案失敗: {str(e)}")

    df["品號"] = df["品號"].astype(str).str.strip()
    df = df[df["品號"].str.endswith("A", na=False)]
    currencies = [c for c in df["幣別名稱"].dropna().unique().tolist() if c != "台幣"]
    return {"currencies": currencies}


@app.post("/process-excel")
async def process_excel(
    files: Optional[List[UploadFile]] = File(default=None),
    months: Optional[List[str]] = Form(default=None),
    inventory_file: Optional[UploadFile] = File(None),
    onboard_normal_file: Optional[UploadFile] = File(None),
    onboard_fly_file: Optional[UploadFile] = File(None),
    stock_file: Optional[UploadFile] = File(None),
    import_file: Optional[UploadFile] = File(None),
    cost_file: Optional[UploadFile] = File(None),
    exchange_rates_json: Optional[str] = Form(None),
):
    has_sales = bool(files and any(f.filename for f in files))
    has_inventory = bool(inventory_file and inventory_file.filename)
    has_onboard_normal = bool(onboard_normal_file and onboard_normal_file.filename)
    has_onboard_fly = bool(onboard_fly_file and onboard_fly_file.filename)
    has_stock = bool(stock_file and stock_file.filename)
    has_import = bool(import_file and import_file.filename)
    has_cost = bool(cost_file and cost_file.filename)

    if has_onboard_normal != has_onboard_fly:
        raise HTTPException(status_code=400, detail="一般航線與串飛航線檔案須同時上傳")

    has_onboard = has_onboard_normal and has_onboard_fly

    if not has_sales and not has_inventory and not has_onboard and not has_stock and not has_import and not has_cost:
        raise HTTPException(status_code=400, detail="請至少上傳一個檔案")

    result = None
    present_months = []

    # ── 處理銷售明細 ──────────────────────────────────────────
    if has_sales:
        if len(files) != len(months or []):
            raise HTTPException(status_code=400, detail="files 與 months 數量不符")

        all_sales = []
        all_names = []

        for file, month_str in zip(files, months):
            if not file.filename.endswith(('.xls', '.xlsx')):
                raise HTTPException(
                    status_code=400,
                    detail=f"檔案 '{file.filename}' 不是有效的 Excel 格式 (.xls 或 .xlsx)"
                )

            month_name = MONTH_NAMES.get(month_str)
            if not month_name:
                raise HTTPException(status_code=400, detail=f"無效的月份：{month_str}")

            contents = await file.read()
            file_stream = io.BytesIO(contents)

            try:
                excel_file = pd.ExcelFile(file_stream)
                sheet_names = excel_file.sheet_names

                if len(sheet_names) == 1:
                    target_sheet = sheet_names[0]
                elif "details" in sheet_names:
                    target_sheet = "details"
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"檔案 '{file.filename}' 包含多個工作表，但找不到名為 'details' 的工作表"
                    )

                df = pd.read_excel(excel_file, sheet_name=target_sheet, dtype={"SKU no": str})
                df = df[["SKU no", "SKU title", "Volume", "Amount"]]
            except KeyError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"檔案 '{file.filename}' 缺少必要欄位：{str(e)}"
                )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"讀取 '{file.filename}' 失敗: {str(e)}")

            df['SKU no'] = df['SKU no'].astype(str).str.zfill(5).str.replace(" ", "", regex=False)

            df_grouped = df.groupby("SKU no").agg({"Volume": "sum", "Amount": "sum"}).reset_index()
            df_grouped = df_grouped.rename(columns={
                "SKU no": "SKU No.",
                "Volume": f"{month_name}銷售量",
                "Amount": f"{month_name}銷售額"
            })
            df_grouped["SKU No."] = df_grouped["SKU No."].astype(str).str.strip()

            df.rename(columns={"SKU no": "SKU No.", "SKU title": "品名"}, inplace=True)
            df["SKU No."] = df["SKU No."].astype(str).str.strip()
            names = df.drop_duplicates(subset=["SKU No."], keep="first")[["SKU No.", "品名"]]
            names = names[names["品名"].notna()]

            all_sales.append(df_grouped)
            all_names.append(names)

        result = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), all_sales)
        name_df = pd.concat(all_names).drop_duplicates(subset=["SKU No."], keep="first")
        result = result.merge(name_df, on="SKU No.", how="left")

        present_months = [m for m in MONTH_ORDER if f"{m}銷售量" in result.columns]
        vol_cols = [f"{m}銷售量" for m in present_months]
        amt_cols = [f"{m}銷售額" for m in present_months]
        result = result[["SKU No.", "品名"] + vol_cols + amt_cols]

    # ── 處理在途庫存 ──────────────────────────────────────────
    df_inv = None
    if has_inventory:
        df_inv = await process_inventory(inventory_file)
        if result is None:
            result = df_inv[["SKU No.", "品名", "在途庫存"]]

    # ── 處理機上量 ────────────────────────────────────────────
    df_onboard = None
    if has_onboard:
        df_onboard = await process_onboard(onboard_normal_file, onboard_fly_file)
        if result is None:
            result = df_onboard

    # ── 處理期末存量 ──────────────────────────────────────────
    df_stock = None
    if has_stock:
        df_stock = await process_stock(stock_file)
        if result is None:
            result = df_stock

    # ── 處理本月進貨 ──────────────────────────────────────────
    df_import = None
    if has_import:
        df_import = await process_import(import_file)
        if result is None:
            result = df_import

    # ── 處理商品成本 ──────────────────────────────────────────
    df_cost = None
    if has_cost:
        if not exchange_rates_json:
            raise HTTPException(status_code=400, detail="請提供匯率資料")
        try:
            exchange_rates = json.loads(exchange_rates_json)
        except Exception:
            raise HTTPException(status_code=400, detail="匯率格式錯誤")
        df_cost = await process_cost(cost_file, exchange_rates)
        if result is None:
            result = df_cost

    # ── 合併為單一工作表 ──────────────────────────────────────────
    # 收集各來源品名，優先順序：銷售明細 > 在途庫存 > 機上量 > 期末存量 > 本月進貨 > 商品成本
    name_sources = []
    for df_n in (([result] if has_sales and result is not None else []) +
                 [df_inv, df_onboard, df_stock, df_import, df_cost]):
        if df_n is not None and "品名" in df_n.columns:
            name_sources.append(df_n[["SKU No.", "品名"]].dropna(subset=["品名"]))
    all_names = (pd.concat(name_sources).drop_duplicates(subset=["SKU No."], keep="first")
                 if name_sources else pd.DataFrame(columns=["SKU No.", "品名"]))

    def drop_name(df):
        return df.drop(columns=["品名"], errors="ignore")

    # Outer join: 銷售明細、在途庫存、機上量、期末存量、本月進貨
    base_dfs = []
    if has_sales and result is not None:
        base_dfs.append(drop_name(result))
    if df_inv is not None:
        base_dfs.append(drop_name(df_inv[["SKU No.", "品名", "在途庫存"]]))
    if df_onboard is not None:
        base_dfs.append(drop_name(df_onboard))
    if df_stock is not None:
        base_dfs.append(drop_name(df_stock))
    if df_import is not None:
        base_dfs.append(drop_name(df_import))

    if base_dfs:
        merged = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), base_dfs)
        # Left join: 商品成本
        if df_cost is not None:
            merged = merged.merge(drop_name(df_cost), on="SKU No.", how="left")
    else:
        # 只有商品成本
        merged = drop_name(df_cost) if df_cost is not None else pd.DataFrame(columns=["SKU No."])

    # 加回品名
    merged = merged.merge(all_names, on="SKU No.", how="left")

    # 欄位排序：SKU No., 品名, TWD成本, 機上量, {月}銷售量/額, 期末存量, 本月進貨, 在途庫存
    sales_cols = [c for c in merged.columns if "銷售量" in c or "銷售額" in c]
    ordered = ["SKU No.", "品名", "TWD成本", "機上量"] + sales_cols + ["期末存量", "本月進貨", "在途庫存"]
    final_cols = [c for c in ordered if c in merged.columns]
    merged = merged[final_cols]

    # ── 輸出 ──────────────────────────────────────────────────
    if present_months:
        month_nums = [MONTH_NUM[m] for m in present_months]
        out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"
    else:
        out_filename = "TTW 庫存表.xlsx"

    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name="報表", index=False)

    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
