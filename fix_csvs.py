"""
fix_csvs.py  —  run from project root:  python fix_csvs.py
Rewrites metrics.csv and dashboard_requirements.csv with correct quoting.
"""
import csv, pathlib

OUT = pathlib.Path("csv_inputs")
OUT.mkdir(exist_ok=True)

# ── 1. metrics.csv ───────────────────────────────────────────────────────
with open(OUT / "metrics.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["metric_id","metric_name","formula","datatype","format_string","description","is_lod"])
    w.writerow(["met_001","Profit Ratio","SUM([Profit])/SUM([Sales])","real","#0.0%","Profit as % of Sales","false"])
    w.writerow(["met_002","Days to Ship","DATEDIFF('day',[Order Date],[Ship Date])","integer","#","Days between order and ship","false"])
    w.writerow(["met_003","# Orders","COUNTD([Order ID])","integer","#","Distinct orders","false"])
    w.writerow(["met_004","# Customers","COUNTD([Customer ID])","integer","#","Distinct customers","false"])
    w.writerow(["met_005","Sales vs PY %","(SUM([Sales])-LOOKUP(SUM([Sales]),-1))/ABS(LOOKUP(SUM([Sales]),-1))","real","+#0.0%;-#0.0%","YoY Sales growth","false"])
    w.writerow(["met_006","Profit vs PY %","(SUM([Profit])-LOOKUP(SUM([Profit]),-1))/ABS(LOOKUP(SUM([Profit]),-1))","real","+#0.0%;-#0.0%","YoY Profit growth","false"])
    w.writerow(["met_007","Is Profitable","IF SUM([Profit]) > 0 THEN 'Profitable' ELSE 'Not Profitable' END","string","","Profitability flag","false"])
    w.writerow(["met_008","Customer Type","IF COUNTD([Order ID]) > 1 THEN 'Repeat Customer' ELSE 'New Customer' END","string","","New or repeat customer","false"])
    w.writerow(["met_009","Sales CY","{ FIXED YEAR([Order Date]) : SUM([Sales]) }","real","$#,##0.0K","Current year sales LOD","true"])
    w.writerow(["met_010","Profit CY","{ FIXED YEAR([Order Date]) : SUM([Profit]) }","real","$#,##0.0K","Current year profit LOD","true"])
print("  wrote metrics.csv")

# ── 2. dashboard_requirements.csv ────────────────────────────────────────
H = ["view_id","view_name","view_type","chart_type","rows","columns","color","size","label",
     "filter_fields","sort_by","sort_direction","dashboard_id","dashboard_layout",
     "views_in_dashboard","width_px","height_px","notes"]

def r(*a):
    assert len(a) == 18, f"row '{a[0]}' has {len(a)} cols, expected 18"
    return list(a)

rows = [
    H,
    r("kpi_sales","KPI Sales","worksheet","Line","MONTH(Order Date)","SUM(Sales)","","","","","","","","","","","","KPI Sales card"),
    r("kpi_profit","KPI Profit","worksheet","Line","MONTH(Order Date)","SUM(Profit)","","","","","","","","","","","","KPI Profit card"),
    r("kpi_orders","KPI Orders","worksheet","Line","MONTH(Order Date)","# Orders","","","","","","","","","","","","KPI Orders card"),
    r("kpi_customers","KPI Customers","worksheet","Line","MONTH(Order Date)","# Customers","","","","","","","","","","","","KPI Customers card"),
    r("sales_by_state","Sales by State","worksheet","Map","","State/Province","SUM(Sales)","SUM(Sales)","SUM(Sales)","","","","","","","","","Filled map"),
    r("sales_by_region","Sales by Region","worksheet","Bar","Region","SUM(Sales)","","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Bar by region"),
    r("sales_by_segment","Sales by Segment","worksheet","Bar","Segment","SUM(Sales)","","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Bar by segment"),
    r("sales_by_category","Sales by Category","worksheet","Bar","Category","SUM(Sales)","","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Bar by category"),
    r("top10_subcategory","Sales by Top 10 Sub-Category","worksheet","Bar","Sub-Category","SUM(Sales)","","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Top 10 sub-categories"),
    r("sales_by_prod_category","Sales by Product Category","worksheet","Bar","Category","SUM(Sales)","SUM(Sales)","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Category with sparkline"),
    r("top18_products","Top 18 Products","worksheet","Bar","Product Name","SUM(Sales)","Is Profitable","","SUM(Sales)","","SUM(Sales)","desc","","","","","","Bar by profitability"),
    r("profit_ratio_top24","Profit Ratio - Top 24 Products","worksheet","Text","Product Name","Profit Ratio|# Orders|SUM(Sales)|SUM(Profit)","","","","","SUM(Sales)","desc","","","","","","Text table top 24"),
    r("sales_by_region_monthly","Sales by Regions Monthly","worksheet","Bar","Region|MONTH(Order Date)","SUM(Sales)","","SUM(Sales)","","","","","","","","","","Small multiples"),
    r("kpis_by_state","KPIs by State","worksheet","Text","State/Province","# Orders|SUM(Sales)|SUM(Profit)|Profit Ratio","Is Profitable","","","","SUM(Sales)","desc","","","","","","KPI table by state"),
    r("order_trend","Order Trend","worksheet","Area","MONTH(Order Date)","# Orders","","","","","","","","","","","","Area chart 2020-2023"),
    r("order_details_table","Order Details Table","worksheet","Text","Order ID|Customer Name|City|State/Province|Segment|Days to Ship|Quantity|SUM(Sales)|SUM(Profit)|Profit Ratio","","","","","","Order Date","asc","","","","","","Order detail table"),
    r("sales_by_cust_segment","Sales by Customer Segment","worksheet","Bar","Segment","SUM(Sales)","SUM(Sales)","SUM(Sales)","SUM(Sales)","","SUM(Sales)","desc","","","","","","Segment bars"),
    r("top15_customers","Top 15 Customers","worksheet","Bar","Customer Name","SUM(Sales)","Is Profitable","","SUM(Sales)","","SUM(Sales)","desc","","","","","","Top 15 customers"),
    r("new_repeat_trend","Trend - New vs Repeat Customer","worksheet","Line","MONTH(Order Date)","# Customers","Customer Type","","","","","","","","","","","New vs repeat trend"),
    r("dash_home","Home","dashboard","","","","","","","","","","dash_home","vertical","kpi_sales|kpi_profit|kpi_orders|kpi_customers|sales_by_state|sales_by_region|sales_by_segment|sales_by_category|top10_subcategory","1280","900","Home dashboard"),
    r("dash_product","Product Analysis","dashboard","","","","","","","","","","dash_product","vertical","kpi_sales|kpi_profit|kpi_orders|kpi_customers|sales_by_prod_category|top18_products|profit_ratio_top24","1280","900","Product dashboard"),
    r("dash_location","Location Analysis","dashboard","","","","","","","","","","dash_location","vertical","kpi_sales|kpi_profit|kpi_orders|kpi_customers|sales_by_region_monthly|kpis_by_state","1280","900","Location dashboard"),
    r("dash_orders","Order Details","dashboard","","","","","","","","","","dash_orders","vertical","order_trend|order_details_table","1280","900","Orders dashboard"),
    r("dash_customers","Customer Analysis","dashboard","","","","","","","","","","dash_customers","vertical","kpi_sales|kpi_profit|kpi_orders|kpi_customers|sales_by_cust_segment|top15_customers|new_repeat_trend","1280","900","Customer dashboard"),
]

with open(OUT / "dashboard_requirements.csv", "w", newline="", encoding="utf-8") as f:
    csv.writer(f).writerows(rows)
print("  wrote dashboard_requirements.csv")

import pandas as pd
m  = pd.read_csv(OUT / "metrics.csv")
dr = pd.read_csv(OUT / "dashboard_requirements.csv")
print(f"\n  metrics.csv               : {len(m)} rows x {len(m.columns)} cols  {'OK' if len(m.columns)==7 else 'FAIL'}")
print(f"  dashboard_requirements.csv: {len(dr)} rows x {len(dr.columns)} cols  {'OK' if len(dr.columns)==18 else 'FAIL'}")
print("\nDone. Now run:  python cli.py validate --csv-dir csv_inputs\\")
