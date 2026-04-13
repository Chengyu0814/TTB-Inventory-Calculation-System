"""虎航：各上傳檔案的讀取與處理函式

每個函式對應一種輸入檔案，回傳整理過、以 SKU No. 為主鍵的 DataFrame。
"""

import io

import pandas as pd
from fastapi import HTTPException, UploadFile


async def process_inventory(file: UploadFile) -> pd.DataFrame: #採購未交量
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


async def process_cost(file: UploadFile, exchange_rates: dict) -> pd.DataFrame: # 品號價格資料 (成本)
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


async def process_import(file: UploadFile) -> pd.DataFrame: # 進貨明細 (本月進貨)
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


async def process_stock(file: UploadFile) -> pd.DataFrame: # 期末存量
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


async def process_onboard(
    normal_file: UploadFile,
    fly_file: UploadFile,
    normal_multiplier: float = 41,
    fly_multiplier: float = 10,
) -> pd.DataFrame: # 機上量
    """處理 機上量 檔案（倍數可由呼叫端覆寫，預設 一般航線 × 41，串飛航線 × 10）"""

    async def read_onboard_file(file: UploadFile, multiplier: float):
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

    df_normal, normal_unique = await read_onboard_file(normal_file, normal_multiplier)
    df_fly, fly_unique = await read_onboard_file(fly_file, fly_multiplier)

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


async def scan_cost_currencies_from_file(cost_file: UploadFile) -> list:
    """預掃描品號價格資料，回傳非台幣的幣別清單"""
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
    return [c for c in df["幣別名稱"].dropna().unique().tolist() if c != "台幣"]
