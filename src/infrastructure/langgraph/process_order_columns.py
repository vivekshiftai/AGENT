"""
Hard-coded dimensions and measures for Process Order view (Gantt / production planning).

Used when column selection bypasses LLM: filtered_analytical_dimensions and
filtered_analytical_measures are built from these lists. Date filter uses
ZPPBSTDT_Process_Order (basic date) from current date.

To fetch data per day: use ZPPBSTDT_Process_Order as the filter column and call
the SAP API once per day (e.g. ZPPBSTDT_Process_Order eq YYYY-MM-DD) for each
date in range; then group by process order, then by line, and ask LLM for
Gantt plan per line and combine.
"""

# Date column for filtering — use for fetch from current date (basic date filter)
PROCESS_ORDER_DATE_COLUMN = "ZPPBSTDT_Process_Order"

# Dimensions (filter/group by)
# - ZPPBSTDT_Process_Order: date column (filter on it)
# - ZRESMRPC_Process_Order: line
# - MATERIAL_Process_Order: material
# - 0PLANT_Process_Order: plant number
# - ZPRDORDL_Process_Order: finished product number
# - 0PRODORDD14: main process order (user wrote PRODORDER∞D14 — verify exact name in SAP)
PROCESS_ORDER_DIMENSIONS = [
    "ZPPBSTDT_Process_Order",   # date — filter on this (basic date)
    "ZRESMRPC_Process_Order",  # line
    "MATERIAL_Process_Order",  # material
    "0PLANT_Process_Order",    # plant number
    "ZPRDORDL_Process_Order", # finished product number
    "0PRODORDD14",            # main process order (check once in SAP)
]

# Main process order column (for Gantt job_id)
PROCESS_ORDER_MAIN_COL = "0PRODORDD14"

# Measures (technical names from Process Order view)
PROCESS_ORDER_MEASURES = [
    "Target_Qty",
    "ZCOCOMAT",
    "ZCOCOMTG",
    "ZCOSUPAT",
    "NET_VALUE",
    "ZCOSUPTG",
    "Process_Order_QtyCS",
    "Process_Order_Item_QtyPAL",
    "ZACTLEAT",
    "Process_Order_Item_QtyEA",
    "Process_Order_Item_QtyLB",
    "ZEXECTIM",
    "Process_Order_Item_QtyKG",
    "0DLV_QTY",
    "0GRS_WGT_DL",
    "0NET_WGT_DL",
    "0NO_DEL_IT",
    "0SHP_PR_TMF",
    "0SHP_PR_TMV",
    "0TM_GROVOL",
    "0VOLUME_DL",
    "TM_DISTANC",
    "TM_NETDURA",
    "ZGITIMEST",
    "Act_Deliv_QtyPAL",
    "Deliv_QtyPAL",
    "Act_Deliv_QtyCS",
    "ZPCACTMN",
    "Deliv_QtyCS",
    "ZPFACTMN",
    "Act_Deliv_QtyEA",
    "Deliv_QtyEA",
    "ZPPACTMN",
    "Act_Deliv_QtyKG",
    "Deliv_QtyKG",
    "Act_Deliv_QtyLB",
    "Deliv_QtyLB",
    "CML_OR_QTY",
    "CML_CF_QTY",
    "NET_PRICE",
    "NET_WT_AP",
    "ZPPORDQT",
    "Cnt_Sales_Ord_Items",
    "ZPPSCHMN",
    "ZPPSCRQ",
    "ZDOCNUMC",
    "SUBTOTAL_5",
    "Sales_QtyOrderedPAL",
    "Sales_QtyConfirmedPAL",
    "Sales_QtyOrderedCS",
    "Sales_QtyConfirmedCS",
    "Sales_QtyOrderedEA",
    "Sales_QtyConfirmedEA",
    "Sales_QtyOrderedLB",
    "ZSCHRELD",
    "Sales_QtyConfirmedLB",
    "Target",
    "ZSTARELD",
    "Z_PPGRQTY",
    "0ACT_DL_QTY",
    "Z_PPIMQTY",
    "ZPCACTMN_LY",
    "Tgt_Attain",
    "Tgt_Adherence",
    "Act_Prod_QtyCS",
    "Process_Order_Count",
    "QUANT_B",
    "ISSTOTSTCK",
    "RECTOTSTCK",
    "ISSVALSTCK",
    "RECVALSTCK",
    "RECTOTSTCK_KG",
    "RECTOTSTCK_LB",
    "RECTOTSTCK_EA",
    "RECTOTSTCK_PAL",
    "RECTOTSTCK_CS",
    "RECVS_VAL",
    "ZVV001",
    "Actual_Units_CS",
    "Actual_Units_DZ",
    "Actual_Units_TI",
    "Actual_Units_EQM",
    "Actual_Units_TR",
    "Actual_Units_EA",
    "Actual_Units_PAL",
    "Actual_Units_LB",
    "Actual_Units_KG",
    "Z_PPPNQTY",
    "Z_PPPNQTY_CS",
    "Z_PPPNQTY_PAL",
    "Z_PPPNQTY_EA",
    "Z_PPPNQTY_DZ",
    "Z_PPPNQTY_TR",
    "Z_PPPNQTY_EQM",
    "Z_PPPNQTY_TI",
    "Receipt_Quantity_Total_Stock_T",
    "Receipt_Quantity_Total_Stock_D",
    "Sales_QtyOrderedDZ",
    "Sales_QtyOrderedTR",
    "Case_Fill_Rate_",
    "Target_CFR_",
    "Rolling_12_Month",
    "Curr_Year_YTD",
    "Prev_Year_YTD",
    "Vs_Prior_Year",
    "Tot_CFR_",
    "Sls_Qty_Ord",
    "Act_Del_Qty_OS_CCS",
    "Sls_Qty_OSC_Ord_Copy",
    "Total_CFR_OSC",
    "Total_CFR__GMC",
    "Completed_PO_Count",
    "Completed_PO_Count_LY",
    "Act_Del_QtyCS_Clsd",
    "Sched_Adher_PO_Count",
    "Adhere_",
    "Adhere__LY",
    "Prod_80120_Count",
    "ShortLead_Sched_Change_Count",
    "Act_Prod_QtyCls",
    "Attainment_",
    "Attainment__LY",
    "Case_Fill_Rate_SO_",
    "Total_CFR_SO_",
    "Total_Prod_Units",
    "Total_Prod_Units_CS",
    "Schedule_QtyAttainCS",
    "Open_Prod_Qty",
    "Total_Prod_Units_EA",
    "Total_Prod_Units_KG",
    "Total_Prod_Units_LB",
    "Total_Prod_Units_PAL",
    "Total_Prod_Units_TR",
    "Total_Prod_Units_DZ",
    "Product_to_Sale_Ratio",
    "Prod__to_Plan",
]
