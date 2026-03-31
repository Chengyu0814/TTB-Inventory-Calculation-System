import io
import re
import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="Excel Processor API", version="1.0.0")

# 允許前端跨域請求
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


def get_month_name(filename: str) -> str:
    """提取檔案名稱中的月份（如 202603... -> 三月）"""
    match = re.search(r'^2026(\d{2})', filename)
    if match:
        month_num = match.group(1)
        # 簡單映射 01~12 到 一月~十二月，確保通用性
        month_map = {
            "01": "一月", "02": "二月", "03": "三月", "04": "四月", 
            "05": "五月", "06": "六月", "07": "七月", "08": "八月", 
            "09": "九月", "10": "十月", "11": "十一月", "12": "十二月"
        }
        return month_map.get(month_num, "總")
    return "總"


@app.post("/process-excel")
async def process_excel(file: UploadFile = File(...)):
    """
    接收前端上傳的 Excel 檔案，進行銷售資料加總與合併，
    並回傳處理後的 Excel 檔案。
    """
    if not file.filename.endswith(('.xls', '.xlsx')):
        raise HTTPException(status_code=400, detail="請上傳有效的 Excel 檔案 (.xls 或 .xlsx)")

    try:
        # 讀取上傳檔案到 BytesIO
        contents = await file.read()
        file_stream = io.BytesIO(contents)

        # 根據檔名決定月份名稱前綴
        month_str = get_month_name(file.filename)
        vol_col = f"{month_str}銷售量"
        amt_col = f"{month_str}銷售額"

        # 先利用 pd.ExcelFile 解析檔案工作表資訊
        excel_file = pd.ExcelFile(file_stream)
        sheet_names = excel_file.sheet_names
        
        # 決定要讀取哪一個工作表
        if len(sheet_names) == 1:
            target_sheet = sheet_names[0]
        else:
            # 有兩個以上的工作表時，尋找 'details'
            if "details" in sheet_names:
                target_sheet = "details"
            else:
                raise ValueError("檔案包含多個工作表，但找不到名為 'details' 的工作表")
                
        # 讀取指定的工作表（套用 dtype 確保 SKU No 不會掉前導零）
        df_mar = pd.read_excel(excel_file, sheet_name=target_sheet, dtype={"SKU no": str})

        # --- 以下為使用者的資料處理邏輯 --- 
        df_mar = df_mar[["SKU no", "SKU title", "Volume", "Amount"]]
        df_mar['SKU no'] = df_mar['SKU no'].astype(str).str.zfill(5).str.replace(" ", "")
        
        # 加總銷售量與金額
        df_mar_grouped = df_mar.groupby("SKU no").agg({
            "Volume": "sum",
            "Amount": "sum"
        }).reset_index()

        # 重新命名欄位
        df_mar_grouped = df_mar_grouped.rename(columns={
            "SKU no": "SKU No.",
            "Volume": vol_col,
            "Amount": amt_col
        })
        df_mar_grouped["SKU No."] = df_mar_grouped["SKU No."].astype(str).str.strip()

        # 從原始資料提取品名對照
        df_mar.rename(columns={"SKU no": "SKU No.", "SKU title": "品名"}, inplace=True)
        df_mar["SKU No."] = df_mar["SKU No."].astype(str).str.strip()
        orders_unique = df_mar.drop_duplicates(subset=['SKU No.'], keep='first')
        
        # 合併結果
        df_mar_res = df_mar_grouped.merge(orders_unique, on='SKU No.', how='left')
        df_mar_res = df_mar_res[["SKU No.", "品名", vol_col, amt_col]]

        # --- 處理完畢，準備匯出 Excel ---
        output_stream = io.BytesIO()
        df_mar_res.to_excel(output_stream, index=False)
        output_stream.seek(0)

        # 產生下載用的檔名（處理_原檔名.xlsx）
        out_filename = f"processed_{file.filename}"

        return StreamingResponse(
            output_stream,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{out_filename}"',
                "Access-Control-Expose-Headers": "Content-Disposition"  # 讓前端能取到檔名
            }
        )

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"上傳的 Excel 檔案缺少必要欄位：{str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"檔案處理失敗: {str(e)}")
