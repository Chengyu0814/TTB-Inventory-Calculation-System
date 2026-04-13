"""虎航：補貨計算邏輯（上傳採購大表後觸發）"""

import math

import numpy as np
import pandas as pd


def calculate_replenishment(row, demand_col, month_col):
    """單一 SKU 的補貨量計算（含規劃性下架與成箱規定）"""
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


def _format_months(value: float) -> str:
    """把需求月數格式化成顯示字串（整數不帶小數點）"""
    return str(int(value)) if float(value).is_integer() else f"{float(value):g}"


def run_replenishment_calculation(
    merged: pd.DataFrame,
    df_org: pd.DataFrame,
    demand_months: float = 1.5,
) -> tuple[pd.DataFrame, str, str]:
    """合併採購大表並執行補貨計算，回傳 (結果DataFrame, 月份名稱, 短期需求月數顯示字串)"""
    months_label = _format_months(demand_months)
    demand_col = f"需求量({months_label})"
    restock_col = f"補貨量(以{months_label}個月)"
    amount_col = f"採購金額({months_label})"
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

    # 需求量 & 補貨量 (短期，依 demand_months)
    df_row[demand_col] = (
        df_row["機上量"].fillna(0)
        + np.maximum(df_row["平均月銷量"].fillna(0), df_row[this_month_col].fillna(0)) * demand_months
    ).round(0)
    df_row[restock_col] = df_row.apply(
        lambda r: calculate_replenishment(r, demand_col, this_month_col), axis=1
    ).round(0)
    df_row[amount_col] = (df_row[restock_col] * df_row["TWD成本"].fillna(0)).round(0)

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
    df_row["追加數量"] = (df_row["補貨量(以lead time)"] - df_row[restock_col]).round(0)

    df_row = df_row[~df_row["SKU No."].astype(str).str.contains("B")]

    return df_row, month_name, months_label
