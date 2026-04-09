import io
import json
import math
from urllib.parse import quote
from typing import List, Optional
from functools import reduce

import numpy as np
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

CAL_SALE_SHEETS      = ['TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']
CAL_INVENTORY_SHEETS = ['TPEKSCI', 'TPEKNCP', 'TSAKNCI', 'KHHKNCI', 'RMQKNCI']


def process_cal_sheets(file_bytes: bytes, sheet_list: list, source_col: str, filename: str) -> pd.DataFrame:
    """讀取指定 sheets，各取 PART_NO + source_col，合併並加總"""
    dfs = {}
    for sheet in sheet_list:
        try:
            df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, engine='openpyxl')
            dfs[sheet] = df[['PART_NO', source_col]].rename(columns={source_col: f'{sheet}_QTY'})
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"'{filename}' 的 sheet '{sheet}' 缺少欄位：{str(e)}")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"讀取 '{filename}' 的 sheet '{sheet}' 失敗：{str(e)}")

    df_merged = dfs[sheet_list[0]]
    for sheet in sheet_list[1:]:
        df_merged = df_merged.merge(dfs[sheet], on='PART_NO', how='outer')

    qty_cols = [f'{sheet}_QTY' for sheet in sheet_list]
    df_merged[qty_cols] = df_merged[qty_cols].fillna(0).astype(int)
    df_merged['Total'] = df_merged[qty_cols].sum(axis=1)
    return df_merged


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


async def process_org(file: UploadFile) -> pd.DataFrame:
    """處理 採購大表 檔案，回傳 SKU No. + 規劃性下架 + 成箱規定 + lead time 的 DataFrame"""
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), header=2)
        df = df[["SKU No.", "DESCRIPTION", "規劃性下架", "成箱規定", "lead time"]]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"採購大表缺少必要欄位：{str(e)}（需要：SKU No., DESCRIPTION, 規劃性下架, 成箱規定, lead time）")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取採購大表失敗: {str(e)}")

    df["SKU No."] = df["SKU No."].astype(str).str.strip()
    return df


def calculate_replenishment(row, demand_col, month_col):
    delisting = row.get("規劃性下架")
    if pd.notna(delisting) and str(delisting).strip() not in ["", "0", "False"]:
        return 0

    def get_num(key):
        val = row.get(key, 0)
        return 0 if pd.isna(val) or val == "" else float(val)

    demand = get_num(demand_col)
    final_inv = get_num("期末存量")
    sales = get_num(month_col)
    restock_this_month = get_num("本月進貨")
    in_transit = get_num("在途庫存")

    x = max(demand - (final_inv - sales) - restock_this_month - in_transit, 0)

    box_rule = row.get("成箱規定")
    if pd.isna(box_rule) or str(box_rule).strip() == "" or float(box_rule) == 0:
        return x
    else:
        box_rule = float(box_rule)
        return math.ceil(x / box_rule) * box_rule


def run_replenishment_calculation(merged: pd.DataFrame, df_org: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """合併採購大表並執行補貨計算，回傳 (結果DataFrame, 月份名稱)"""
    df_org_subset = df_org[["SKU No.", "DESCRIPTION", "規劃性下架", "成箱規定", "lead time"]].copy()
    df_org_subset["SKU No."] = df_org_subset["SKU No."].astype(str).str.strip()

    df_row = pd.merge(merged, df_org_subset, on="SKU No.", how="left")

    # 若 DESCRIPTION 欄存在則補品名（採購大表有時帶 DESCRIPTION）
    if "DESCRIPTION" in df_row.columns:
        df_row["品名"] = df_row["品名"].where(
            df_row["品名"].notna() & (df_row["品名"].str.strip() != ""),
            df_row["DESCRIPTION"]
        )
        df_row = df_row.drop(columns=["DESCRIPTION"])

    df_row["成箱規定"] = df_row["成箱規定"].fillna(5)

    sales_cols = df_row.filter(like="月銷售量").columns
    if len(sales_cols) > 0:
        df_row["平均月銷量"] = df_row[sales_cols].mean(axis=1).round(0)
        last_month_col = sales_cols[-1]
        month_name = last_month_col.replace("銷售量", "")
        df_row[f"本月銷貨({month_name})"] = df_row[last_month_col]
    else:
        df_row["平均月銷量"] = 0
        month_name = ""
        df_row[f"本月銷貨({month_name})"] = 0

    revenue_cols = df_row.filter(like="月銷售額").columns.tolist()
    sales_cols_list = sales_cols.tolist()
    this_month_col = f"本月銷貨({month_name})"
    new_col_order = (
        ["SKU No.", "品名", "規劃性下架", "成箱規定", "TWD成本", "lead time", "機上量"]
        + sales_cols_list + revenue_cols
        + ["平均月銷量", "期末存量", this_month_col, "本月進貨", "在途庫存"]
    )
    new_col_order = [c for c in new_col_order if c in df_row.columns]
    df_row = df_row[new_col_order]

    # 需求量 & 補貨量 (1.5個月)
    df_row["需求量(1.5)"] = (
        df_row["機上量"].fillna(0)
        + np.maximum(df_row["平均月銷量"].fillna(0), df_row[this_month_col].fillna(0)) * 1.5
    ).round(0)
    df_row["補貨量(以1.5個月)"] = df_row.apply(
        lambda r: calculate_replenishment(r, "需求量(1.5)", this_month_col), axis=1
    ).round(0)
    df_row["採購金額(1.5)"] = (df_row["補貨量(以1.5個月)"] * df_row["TWD成本"].fillna(0)).round(0)

    # 需求量 & 補貨量 (lead time)
    df_row["需求量_lead_time"] = (
        df_row["機上量"].fillna(0)
        + np.maximum(df_row["平均月銷量"].fillna(0), df_row[this_month_col].fillna(0))
        * df_row["lead time"].fillna(0)
    ).round(0)
    df_row["補貨量(以lead time)"] = df_row.apply(
        lambda r: calculate_replenishment(r, "需求量_lead_time", this_month_col), axis=1
    ).round(0)
    df_row["採購金額(lead time)"] = (df_row["補貨量(以lead time)"] * df_row["TWD成本"].fillna(0)).round(0)
    df_row["追加數量"] = (df_row["補貨量(以lead time)"] - df_row["補貨量(以1.5個月)"]).round(0)

    df_row = df_row[~df_row["SKU No."].astype(str).str.contains("B")]

    return df_row, month_name


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
    org_file: Optional[UploadFile] = File(None),
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

    # ── 採購大表：補貨計算 ─────────────────────────────────────
    has_org = bool(org_file and org_file.filename)
    if has_org:
        df_org = await process_org(org_file)
        merged, calc_month = run_replenishment_calculation(merged, df_org)
        if not present_months and calc_month:
            out_month = calc_month
        elif present_months:
            out_month = present_months[-1]
        else:
            out_month = ""
    else:
        out_month = present_months[-1] if present_months else ""

    # ── 輸出 ──────────────────────────────────────────────────
    if has_org:
        out_filename = f"虎航庫存計算表({out_month}).xlsx" if out_month else "虎航庫存計算表.xlsx"
    elif present_months:
        month_nums = [MONTH_NUM[m] for m in present_months]
        out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"
    else:
        out_filename = "TTW 庫存表.xlsx"

    output_stream = io.BytesIO()
    if has_org:
        with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
            merged.to_excel(writer, sheet_name="Main", index=False)
            worksheet = writer.sheets["Main"]
            descriptions = {
                "機上量": "DFS一般航線裝載表 × 41 + 串飛航線裝載表 × 10",
                "平均月銷量": "所有月份的銷售量平均",
                f"本月銷貨({out_month})": "最新月份的銷售量",
                "需求量(1.5)": "機上量+MAX(平均月銷量,本月銷貨)*1.5",
                "補貨量(以1.5個月)": "MAX(需求量-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                "採購金額(1.5)": "補貨量(以1.5個月)*TWD成本",
                "需求量_lead_time": "機上量+MAX(平均月銷量,本月銷貨)*lead time",
                "補貨量(以lead time)": "MAX(需求量_lead_time-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                "採購金額(lead time)": "補貨量(以lead time)*TWD成本",
                "追加數量": "補貨量(以lead time)-補貨量(以1.5個月)",
            }
            for col_num, col_name in enumerate(merged.columns):
                if col_name in descriptions:
                    worksheet.write_comment(0, col_num, descriptions[col_name])
    else:
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


@app.post("/cal/process-excel")
async def cal_process_excel(
    files: List[UploadFile] = File(...),
    months: List[str] = Form(...),
):
    if len(files) != len(months):
        raise HTTPException(status_code=400, detail="files 與 months 數量不符")

    month_dfs = []
    present_months = []

    for file, month_str in zip(files, months):
        month_name = MONTH_NAMES.get(month_str)
        if not month_name:
            raise HTTPException(status_code=400, detail=f"無效的月份：{month_str}")

        contents = await file.read()

        df_inv  = process_cal_sheets(contents, CAL_INVENTORY_SHEETS, 'END_TTL_QTY', file.filename)
        df_sale = process_cal_sheets(contents, CAL_SALE_SHEETS,      'CS_QTY',      file.filename)

        # 合併庫存與銷售，重疊欄位加後綴
        df_month = df_inv.merge(df_sale, on='PART_NO', how='outer', suffixes=('_庫存', '_銷售'))
        df_month = df_month.fillna(0)

        # 整理欄位名稱：_QTY_庫存 → _庫存、_QTY（只在庫存）→ _庫存、Total_庫存 → 庫存合計
        col_rename = {}
        for col in df_month.columns:
            if col == 'PART_NO':
                continue
            new = col
            new = new.replace('_QTY_庫存', '_庫存')
            new = new.replace('_QTY_銷售', '_銷售')
            new = new.replace('_QTY',     '_庫存')   # TPEKSCI_QTY（只出現在庫存）
            new = new.replace('Total_庫存', '庫存合計')
            new = new.replace('Total_銷售', '銷售合計')
            col_rename[col] = new
        df_month = df_month.rename(columns=col_rename)

        # 數值欄位轉 int，再加月份前綴
        num_cols = [c for c in df_month.columns if c != 'PART_NO']
        df_month[num_cols] = df_month[num_cols].astype(int)
        df_month = df_month.rename(columns={c: f'{month_name}_{c}' for c in num_cols})

        month_dfs.append(df_month)
        present_months.append(month_name)

    # 所有月份以 PART_NO outer join 合併成一張表
    merged = reduce(lambda l, r: l.merge(r, on='PART_NO', how='outer'), month_dfs)
    merged = merged.fillna(0)

    # 轉 int、去除空白 PART_NO、排序
    for col in merged.columns:
        if col != 'PART_NO':
            merged[col] = merged[col].astype(int)
    merged = merged.dropna(subset=['PART_NO'])
    merged = merged[merged['PART_NO'].astype(str).str.strip() != '']
    merged = merged.sort_values(by='PART_NO').reset_index(drop=True)

    # 輸出檔名用最新月份
    present_months_sorted = [m for m in MONTH_ORDER if m in present_months]
    out_month   = present_months_sorted[-1]
    out_filename = f'華航庫存計算表({out_month}).xlsx'

    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine='openpyxl') as writer:
        merged.to_excel(writer, sheet_name='報表', index=False)
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
