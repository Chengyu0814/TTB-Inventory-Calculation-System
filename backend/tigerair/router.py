"""虎航：API endpoints

對應前端 tigerair.html / app.js。
"""

import io
import json
from functools import reduce
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from constants import MONTH_NAMES, MONTH_NUM, MONTH_ORDER
from responses import make_xlsx_response

from .processors import (
    process_cost,
    process_import,
    process_inventory,
    process_onboard,
    process_org,
    process_stock,
    scan_cost_currencies_from_file,
)
from .replenishment import run_replenishment_calculation


router = APIRouter()


@router.post("/scan-cost-currencies")
async def scan_cost_currencies(cost_file: UploadFile = File(...)):
    """處理幣別、匯率轉換的預掃描"""
    currencies = await scan_cost_currencies_from_file(cost_file)
    return {"currencies": currencies}


@router.post("/process-excel")
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
    normal_multiplier: float = Form(41),
    fly_multiplier: float = Form(10),
    demand_months: float = Form(1.5),
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
        df_onboard = await process_onboard(
            onboard_normal_file, onboard_fly_file,
            normal_multiplier=normal_multiplier,
            fly_multiplier=fly_multiplier,
        )
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
    months_label = ""
    if has_org:
        df_org = await process_org(org_file)
        merged, calc_month, months_label = run_replenishment_calculation(
            merged, df_org, demand_months=demand_months
        )
        if not present_months and calc_month:
            out_month = calc_month
        elif present_months:
            out_month = present_months[-1]
        else:
            out_month = ""
    else:
        out_month = present_months[-1] if present_months else ""

    # ── 輸出檔名 ───────────────────────────────────────────────
    if has_org:
        out_filename = f"虎航庫存計算表({out_month}).xlsx" if out_month else "虎航庫存計算表.xlsx"
    elif present_months:
        month_nums = [MONTH_NUM[m] for m in present_months]
        out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"
    else:
        out_filename = "TTW 庫存表.xlsx"

    # ── 寫入 Excel ─────────────────────────────────────────────
    output_stream = _write_tigerair_excel(
        merged, has_org, out_month,
        normal_multiplier=normal_multiplier,
        fly_multiplier=fly_multiplier,
        months_label=months_label,
    )
    return make_xlsx_response(output_stream, out_filename)


def _fmt_num(value: float) -> str:
    """倍數顯示：整數不帶小數點"""
    return str(int(value)) if float(value).is_integer() else f"{float(value):g}"


def _write_tigerair_excel(
    merged: pd.DataFrame,
    has_org: bool,
    out_month: str,
    normal_multiplier: float = 41,
    fly_multiplier: float = 10,
    months_label: str = "1.5",
) -> io.BytesIO:
    """將彙整結果寫入 xlsx；補貨報表會在欄位標題加 cell comment 說明計算公式"""
    output_stream = io.BytesIO()
    nm = _fmt_num(normal_multiplier)
    fm = _fmt_num(fly_multiplier)
    if has_org:
        with pd.ExcelWriter(output_stream, engine="xlsxwriter") as writer:
            merged.to_excel(writer, sheet_name="Main", index=False)
            worksheet = writer.sheets["Main"]
            descriptions = {
                "機上量": f"DFS一般航線裝載表 × {nm} + 串飛航線裝載表 × {fm}",
                "平均月銷量": "所有月份的銷售量平均",
                f"本月銷貨({out_month})": "最新月份的銷售量",
                f"需求量({months_label})": f"機上量+MAX(平均月銷量,本月銷貨)*{months_label}",
                f"補貨量(以{months_label}個月)": "MAX(需求量-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                f"採購金額({months_label})": f"補貨量(以{months_label}個月)*TWD成本",
                "需求量_lead_time": "機上量+MAX(平均月銷量,本月銷貨)*lead time",
                "補貨量(以lead time)": "MAX(需求量_lead_time-(期末存量-本月銷貨)-本月進貨-在途庫存,0) ，且有考慮成箱規定與規劃性下架",
                "採購金額(lead time)": "補貨量(以lead time)*TWD成本",
                "追加數量": f"補貨量(以lead time)-補貨量(以{months_label}個月)",
            }
            for col_num, col_name in enumerate(merged.columns):
                if col_name in descriptions:
                    worksheet.write_comment(0, col_num, descriptions[col_name])
    else:
        with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
            merged.to_excel(writer, sheet_name="報表", index=False)

    output_stream.seek(0)
    return output_stream
