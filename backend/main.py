import io
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
    """處理採購未交量檔案，回傳 SKU No. + 在途庫存 的 DataFrame"""
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
    # 刪除 SKU No. 包含中文字的資料列
    df = df[~df["SKU No."].str.contains(r'[\u4e00-\u9fff]', regex=True)]
    # 只保留交貨庫為華膳-IT
    df = df[df["交貨庫"] == "華膳-IT"]
    # 只保留 SKU No. 以 A 結尾的列
    df = df[df["SKU No."].str.endswith("A", na=False)]
    # 去除尾端的 A，取前 5 碼
    df["SKU No."] = df["SKU No."].str.extract(r'(.{5})A$', expand=False)
    df["SKU No."] = df["SKU No."].astype(str).str.strip()

    df_res = df.groupby("SKU No.", as_index=False).agg({
        "在途庫存": "sum"
    })
    return df_res[["SKU No.", "在途庫存"]]


@app.post("/process-excel")
async def process_excel(
    files: List[UploadFile] = File(...),
    months: List[str] = Form(...),
    inventory_file: Optional[UploadFile] = File(None)
):
    """
    接收一或多個月份的 Excel 銷售明細，各自加總後 outer join。
    可選傳入採購未交量檔案，合併在途庫存欄位。
    回傳 TTW sales summary MM-MM.xlsx。
    """
    if not files:
        raise HTTPException(status_code=400, detail="請至少上傳一個檔案")
    if len(files) != len(months):
        raise HTTPException(status_code=400, detail="files 與 months 數量不符")

    all_sales = []  # 每月: SKU No., month銷售量, month銷售額
    all_names = []  # 每月: SKU No., 品名

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

    # Outer join 所有月份資料
    result = reduce(lambda l, r: l.merge(r, on="SKU No.", how="outer"), all_sales)

    # 品名：取所有月份中第一個非 null 的值
    name_df = pd.concat(all_names).drop_duplicates(subset=["SKU No."], keep="first")
    result = result.merge(name_df, on="SKU No.", how="left")

    # 整理欄位順序：SKU No., 品名, 各月銷售量..., 各月銷售額...
    present_months = [m for m in MONTH_ORDER if f"{m}銷售量" in result.columns]
    vol_cols = [f"{m}銷售量" for m in present_months]
    amt_cols = [f"{m}銷售額" for m in present_months]
    final_cols = ["SKU No.", "品名"] + vol_cols + amt_cols

    # 合併在途庫存（若有上傳）
    if inventory_file and inventory_file.filename:
        df_inv = await process_inventory(inventory_file)
        result = result.merge(df_inv, on="SKU No.", how="left")
        final_cols.append("在途庫存")

    result = result[final_cols]

    # 輸出檔名
    month_nums = [MONTH_NUM[m] for m in present_months]
    out_filename = f"TTW sales summary {month_nums[0]}-{month_nums[-1]}.xlsx"

    output_stream = io.BytesIO()
    result.to_excel(output_stream, index=False)
    output_stream.seek(0)

    return StreamingResponse(
        output_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{out_filename}"',
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
    )
