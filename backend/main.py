import io
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


@app.post("/process-excel")
async def process_excel(
    files: Optional[List[UploadFile]] = File(default=None),
    months: Optional[List[str]] = Form(default=None),
    inventory_file: Optional[UploadFile] = File(None),
    onboard_normal_file: Optional[UploadFile] = File(None),
    onboard_fly_file: Optional[UploadFile] = File(None),
):
    has_sales = bool(files and any(f.filename for f in files))
    has_inventory = bool(inventory_file and inventory_file.filename)
    has_onboard_normal = bool(onboard_normal_file and onboard_normal_file.filename)
    has_onboard_fly = bool(onboard_fly_file and onboard_fly_file.filename)

    if has_onboard_normal != has_onboard_fly:
        raise HTTPException(status_code=400, detail="一般航線與串飛航線檔案須同時上傳")

    has_onboard = has_onboard_normal and has_onboard_fly

    if not has_sales and not has_inventory and not has_onboard:
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

    # ── 輸出 ──────────────────────────────────────────────────
    if present_months:
        month_nums = [MONTH_NUM[m] for m in present_months]
        out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"
    else:
        out_filename = "TTW 庫存表.xlsx"

    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine="openpyxl") as writer:
        if has_sales:
            result.to_excel(writer, sheet_name="銷售明細", index=False)
        if df_inv is not None:
            df_inv[["SKU No.", "品名", "在途庫存"]].to_excel(writer, sheet_name="在途庫存", index=False)
        if df_onboard is not None:
            df_onboard.to_excel(writer, sheet_name="機上量", index=False)
        if not has_sales and df_inv is None and df_onboard is None:
            result.to_excel(writer, sheet_name="Sheet1", index=False)

    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(out_filename)}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
